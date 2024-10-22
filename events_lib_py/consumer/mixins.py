import logging
from typing import Optional

from confluent_kafka import Message, TopicPartition

from events_lib_py import config
from events_lib_py.constants import KafkaConsumerControllerFlagName
from events_lib_py.dataclasses import ConsumerBatchCounter, EventHandlerResponse
from events_lib_py.healthcheck import HealthCheckUtil
from events_lib_py.metrics import KAFKA_MESSAGE_SENT_TO_DLQ_TOTAL
from events_lib_py.pb.event_pb2 import Event

LOGGER = logging.getLogger(__name__)


class KafkaConsumerHandlerMixin:
    def _handle_retry(self, msg: Message, event: Event):
        from events_lib_py import send_event

        retry_count = event.retry_count + 1
        LOGGER.info(
            "msg=%s key=%s event_name=%s retry_count=%s",
            "Pushing message to retry topic",
            msg.key().decode(),
            event.name,
            retry_count,
        )

        send_event(
            topic=self._config.retry_topic,
            name=event.name,
            payload=event.payload,
            retry_count=retry_count,
        )

    def _handle_dlq(
        self,
        msg: Message,
        event_name: Optional[str] = None,
        err_msg: Optional[str] = None,
        exc: Optional[Exception] = None,
    ):
        from events_lib_py import send_to_dlq

        LOGGER.info(
            "msg=%s key=%s event_name=%s err_msg=%s exc=%s",
            "Pushing message to DLQ",
            msg.key().decode(),
            event_name,
            err_msg,
            exc,
            exc_info=True,
        )

        if pre_send_hook := self._config.dlq_pre_send_hook:
            pre_send_hook(msg, event_name, err_msg, exc)

        send_to_dlq(
            dlq_topic=self._config.dlq_topic,
            msg=msg,
            event_name=event_name,
        )

        KAFKA_MESSAGE_SENT_TO_DLQ_TOTAL.inc()

    def process_message(self, msg: Message) -> EventHandlerResponse:
        """
        1. Parse Event
            If failed, pushes event to DLQ
        2. Fetch event handler
            If not available, pushes event to DLQ
        3. Execute handler
            If success, returns
            If retry, pushes event to retry topic
            If dlq, pushes event to DLQ topic
        """
        key = msg.key().decode()
        LOGGER.info("msg=%s key=%s", "Processing message", key)

        try:
            event: Event = Event.FromString(msg.value())
        except Exception as e:
            self._handle_dlq(
                msg=msg,
                err_msg="Unable to parse event",
                exc=e,
            )
            return EventHandlerResponse(dlq=True)

        handler = self._config.event_handler_map.get(event.name)
        if handler is None:
            self._handle_dlq(
                msg=msg,
                event_name=event.name,
                err_msg="Event handler not found",
            )
            return EventHandlerResponse(dlq=True)

        try:
            response = handler(key, event.payload)
            if response.success:
                LOGGER.info("msg=%s key=%s", "Processed message successfully", key)
                return response

            if response.retry:
                max_retries = self._config.max_retries_per_event_map.get(event.name, 0)
                if event.retry_count > max_retries:
                    self._handle_dlq(
                        msg=msg,
                        event_name=event.name,
                        err_msg="Max retries limit reached",
                    )
                    return EventHandlerResponse(dlq=True)
                else:
                    self._handle_retry(msg=msg, event=event)
                    return response

            if response.dlq:
                self._handle_dlq(
                    msg=msg,
                    event_name=event.name,
                    err_msg="Event handler sent for DLQ",
                    exc=response.exception,
                )
                return response

        except Exception as e:
            self._handle_dlq(
                msg=msg,
                event_name=event.name,
                err_msg="Error occurred while processing event",
                exc=e,
            )
            return EventHandlerResponse(dlq=True)


class KafkaConsumerHealthCheckMixin:
    def _generate_offset_maps(self) -> "tuple[dict, dict]":
        assignments: "list[TopicPartition]" = self._consumer.assignment()
        committed = self._consumer.committed(assignments)

        latest_offsets, committed_offsets = {}, {}
        for tp in committed:
            key = (tp.topic, tp.partition)
            committed_offsets[key] = tp.offset

            _, high = self._consumer.get_watermark_offsets(tp)
            latest_offsets[key] = high

        return latest_offsets, committed_offsets

    def is_healthy(self) -> bool:
        latest_offsets, committed_offsets = self._generate_offset_maps()
        healthy = HealthCheckUtil.is_consumer_healthy(
            prev_committed_offsets=self._prev_committed_offsets,
            latest_offsets=latest_offsets,
            committed_offsets=committed_offsets,
        )
        self._prev_committed_offsets = committed_offsets
        return healthy


class KafkaConsumerRateControlMixin:
    def _notify_controller(self, flag_name: str, flag_value: int):
        from .notification import ControllerFlagNotification

        ControllerFlagNotification(
            controller_topic=self._config.controller_topic,
            flag_name=flag_name,
            flag_value=flag_value,
        ).notify_controller()

    def init_batch_counter(self):
        self.batch_counter = ConsumerBatchCounter()

    def is_failed_batch(self, results: "list[EventHandlerResponse]") -> bool:
        """
        Evaluates consideration of current processed batch as failed.
        """
        # Fetch count of retried events
        retried_event_count = 0
        for result in results:
            if result.retry:
                retried_event_count += 1

        return (retried_event_count / self._config.batch_size) >= (
            config.CONSUMER_CONTROLLER_CONFIG.batch_failure_event_percentage / 100
        )

    def evaluate_exponential_backoff(self) -> Optional[int]:
        """
        Evaluates conditions for triggering exponential backoff.
        Returns backoff time in seconds if conditions are met.
        """

        LOGGER.info(
            "msg=%s consecutive_failed_batch_count=%s",
            "Batch considered as failed",
            self.batch_counter.consecutive_failed_batch_count + 1,
        )

        # Disable retry consumer when first failed batch received
        if self.batch_counter.consecutive_failed_batch_count == 0:
            # Sending notification to controller topic to disable retry consumer
            self._notify_controller(
                KafkaConsumerControllerFlagName.retry_consumer_enabled, 0
            )

        # Increment consecutive failed batch counter and check if it reached exponential backoff threshold
        self.batch_counter.consecutive_failed_batch_count += 1
        if (
            self.batch_counter.consecutive_failed_batch_count
            < config.CONSUMER_CONTROLLER_CONFIG.throttle_after_failed_batch_threshold
        ):
            # Not reached exponential backoff threshold
            return

        # Slice batch size if required
        if self._config.batch_size > config.CONSUMER_CONTROLLER_CONFIG.min_batch_size:
            self._config.batch_size = max(
                round(
                    self._config.batch_size
                    * (
                        1
                        - (
                            config.CONSUMER_CONTROLLER_CONFIG.batch_size_slice_percentage
                            / 100
                        )
                    )
                ),
                config.CONSUMER_CONTROLLER_CONFIG.min_batch_size,
            )
            LOGGER.info(
                "msg=%s batch_size=%s", "Sliced batch size", self._config.batch_size
            )

        # Check if exponential backoff is enabled
        if config.CONSUMER_CONTROLLER_CONFIG.exponential_backoff_enabled == 0:
            # Not enabled
            return

        # Evaluating exponential backoff duration in sec
        backoff = config.CONSUMER_CONTROLLER_CONFIG.exponential_backoff_initial_delay
        if self.batch_counter.consecutive_failed_batch_count != 1:
            coeff = config.CONSUMER_CONTROLLER_CONFIG.exponential_backoff_coefficient
            backoff = coeff * pow(
                coeff,
                self.batch_counter.consecutive_failed_batch_count
                - config.CONSUMER_CONTROLLER_CONFIG.throttle_after_failed_batch_threshold,
            )

        return min(
            backoff, config.CONSUMER_CONTROLLER_CONFIG.exponential_backoff_max_delay
        )

    def check_if_system_recovered(self):
        """
        Checks if system has fully recovered to process events. If yes,
        resets consecutive failed batch count and notifies controller
        to enable retry consumer.
        """
        # Increment consecurive success batch post recovery counter and check if system
        # can be considered fully recovered, i.e. batches are being processed successfully
        self.batch_counter.consecutive_success_batch_post_recovery_count += 1
        LOGGER.info(
            "msg=%s consecutive_success_batch_post_recovery_count=%s",
            "Batch processed successfully post recovery",
            self.batch_counter.consecutive_success_batch_post_recovery_count,
        )

        if (
            self.batch_counter.consecutive_success_batch_post_recovery_count
            < config.CONSUMER_CONTROLLER_CONFIG.reset_throttle_after_success_batch_threshold
        ):
            # System can't be considered fully recovered yet
            return

        self.batch_counter.consecutive_failed_batch_count = 0
        LOGGER.info("msg=%s", "System considered as recovered")

        # Sending notification to controller topic to enable retry consumer
        self._notify_controller(
            KafkaConsumerControllerFlagName.retry_consumer_enabled, 1
        )

    def restore_batch_size(self):
        """
        Incremently restore batch size after system recovery. Once
        restored, reset consecutive success batch post recovery count.
        """
        self.batch_counter.consecutive_success_batch_post_recovery_count += 1
        self._config.batch_size = min(
            round(
                self._config.batch_size
                * (
                    1
                    + (
                        config.CONSUMER_CONTROLLER_CONFIG.batch_size_restore_percentage
                        / 100
                    )
                )
            ),
            config.CONSUMER_CONTROLLER_CONFIG.batch_size,
        )
        LOGGER.info(
            "msg=%s batch_size=%s", "Restoring batch size", self._config.batch_size
        )
        if self._config.batch_size == config.CONSUMER_CONTROLLER_CONFIG.batch_size:
            self.batch_counter.consecutive_success_batch_post_recovery_count = 0
