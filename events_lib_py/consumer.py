import logging
import time
from dataclasses import dataclass
from threading import Thread
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from gevent.pool import Pool

from events_lib_py import config
from events_lib_py.constants import ConsumerMode, KafkaConsumerControllerFlagName
from events_lib_py.healthcheck import HealthCheckUtil

from .dataclasses import ConsumerBatchCounter, EventHandlerResponse
from .metrics import (
    KAFKA_CONSUMER_BATCH_FETCH_LATENCY,
    KAFKA_CONSUMER_BATCH_PROCESSING_LATENCY,
    KAFKA_MESSAGE_SENT_TO_DLQ_TOTAL,
)
from .pb.event_pb2 import Event

LOGGER = logging.getLogger(__name__)


@dataclass
class KafkaConsumerConfig:
    mode: str
    group_id: str
    controller_topic: str
    topics: "list[str]"
    retry_topic: str
    dlq_topic: str
    event_handler_map: "dict[str, Callable[[str, bytes], EventHandlerResponse]]"
    max_retries_per_event_map: "dict[str, int]"

    bootstrap_servers: str = "127.0.0.1:9092"
    enable_ssl: bool = True
    auto_commit: bool = False
    auto_offset_reset: str = "earliest"
    session_timeout_in_ms: int = 6000
    batch_size: int = 10
    dlq_pre_send_hook: Optional[
        Callable[[Message, Optional[str], Optional[str], Optional[Exception]], None]
    ] = None
    generic_exception_handler: Optional[Callable[[Exception], None]] = None

    def __post_init__(self):
        if not self.generic_exception_handler:
            self.generic_exception_handler = lambda _: ...

    def to_confluent_config(self) -> dict:
        return {
            "security.protocol": "SSL" if self.enable_ssl else "PLAINTEXT",
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": self.auto_commit,
            "auto.offset.reset": self.auto_offset_reset,
            "session.timeout.ms": self.session_timeout_in_ms,
        }


class _KafkaConsumerHandlerMixin:
    def _handle_retry(self, msg: Message, event: Event):
        from . import send_event

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
        from . import send_to_dlq

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


class _KafkaConsumerRateControlMixin:
    def _notify_controller(self, flag_name: str, flag_value: int):
        from .controller import KafkaConsumerControllerFlagNotification

        KafkaConsumerControllerFlagNotification(
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
        if self._config.batch_size == config.CONSUMER_CONTROLLER_CONFIG.batch_size:
            self.batch_counter.consecutive_success_batch_post_recovery_count = 0


class KafkaConsumer(_KafkaConsumerHandlerMixin, _KafkaConsumerRateControlMixin, Thread):
    entity = "consumer"
    gevent_pool_size = 1000

    def __init__(self, config: KafkaConsumerConfig):
        LOGGER.info("msg=%s entity=%s", "Initializing loop", self.entity)

        super().__init__()
        self._config = config
        self._consumer = Consumer(config.to_confluent_config())
        self._check_broker_connection()
        self._subscribe_topic()
        self._pool = Pool(size=self.gevent_pool_size)
        self._keep_running = True
        self._prev_committed_offsets: "dict[tuple, int]" = {}

        self.exception_handler = config.generic_exception_handler

        if self._config.mode == ConsumerMode.MAIN:
            self.init_batch_counter()

    def shutdown(self):
        LOGGER.info("msg=%s entity=%s", "Stopping loop", self.entity)
        self._keep_running = False

    def _check_broker_connection(self):
        LOGGER.info("msg=%s", "Checking broker connection...")
        metadata = self._consumer.list_topics(timeout=5)
        LOGGER.info(
            "msg=%s topics=%s", "Connected to broker", ", ".join(metadata.topics.keys())
        )

    def _subscribe_topic(self):
        def on_assign(_: Consumer, partitions: "list[TopicPartition]"):
            LOGGER.info("msg=%s partitions=%s", "Assigned partitions", partitions)
            # Resetting previous committed offsets since
            # another partition can be reassigned to the
            # consumer.
            self._prev_committed_offsets = {}

        self._consumer.subscribe(self._config.topics, on_assign=on_assign)

    def is_controller(self) -> bool:
        return self._config.mode == ConsumerMode.CONTROLLER

    def _handle_partition_end_reached(self):
        """
        Should comprise of code that is to be executed when
        committed offset of consumer is the same as latest message
        offset of the assigned partition.
        """
        if self._config.mode == ConsumerMode.RETRY:
            # Sending notification to controller topic to disable retry consumer
            self._notify_controller(
                KafkaConsumerControllerFlagName.retry_consumer_enabled, 0
            )

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

    @KAFKA_CONSUMER_BATCH_FETCH_LATENCY.time()
    def _fetch_batch(self) -> "list[Message]":
        return self._consumer.consume(self._config.batch_size, timeout=1)

    @KAFKA_CONSUMER_BATCH_PROCESSING_LATENCY.time()
    def _exec_batch(self, batch: "list[Message]"):
        to_be_processed_messages: "list[Message]" = []
        for msg in batch:
            if err := msg.error():
                if err.code() == KafkaError._PARTITION_EOF:
                    LOGGER.info(
                        "msg=%s partition=%s",
                        "End of partition reached",
                        msg.partition(),
                    )
                    self._handle_partition_end_reached()
                else:
                    self.exception_handler(Exception(err))
                continue
            to_be_processed_messages.append(msg)

        if not to_be_processed_messages:
            return

        try:
            # Process batch using greenlet-based cooperative multitasking
            results = self._pool.map(self.process_message, to_be_processed_messages)
            # Commit offset in sync after current batch finishes processing
            self._consumer.commit(asynchronous=False)

            if self._config.mode == ConsumerMode.MAIN:
                # Evaluate results of processed batch and slow down consumption
                # rate in case batch was identified as failed.
                if self.is_failed_batch(results=results):
                    LOGGER.info(
                        "msg=%s consecutive_failed_batch_count=%s",
                        "Batch considered as failed",
                        self.batch_counter.consecutive_failed_batch_count + 1,
                    )
                    backoff = self.evaluate_exponential_backoff()
                    if backoff:
                        LOGGER.info(
                            "msg=%s seconds=%s",
                            "Exponential backoff triggered",
                            backoff,
                        )
                        time.sleep(backoff)
                elif self.batch_counter.consecutive_failed_batch_count != 0:
                    self.check_if_system_recovered()
                elif (
                    self.batch_counter.consecutive_success_batch_post_recovery_count
                    != 0
                ):
                    self.restore_batch_size()
        except Exception as e:
            LOGGER.error(
                "msg=%s exc=%s",
                "Exception occurred during batch processing",
                e,
                exc_info=True,
            )
            self.exception_handler(e)

    def run(self):
        LOGGER.info("msg=%s entity=%s", "Starting loop", self.entity)

        try:
            while self._keep_running:
                if (
                    self._config.mode == ConsumerMode.RETRY
                    and config.CONSUMER_CONTROLLER_FLAG.retry_consumer_enabled == 0
                ):
                    # Keep retry consumer blocked until it's enabled
                    time.sleep(5)
                    continue

                batch = self._fetch_batch()

                if not batch:
                    self._handle_partition_end_reached()
                    time.sleep(0.1)
                    continue

                self._exec_batch(batch=batch)
        except Exception as e:
            LOGGER.error(e, exc_info=True)
            self.exception_handler(e)

        self._consumer.close()
        LOGGER.info("msg=%s entity=%s", "Loop stopped", self.entity)
