import logging
import time
from dataclasses import dataclass
from threading import Thread
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from gevent.pool import Pool

from events_lib_py.constants import ConsumerMode
from events_lib_py.healthcheck import HealthCheckUtil

from .dataclasses import EventHandlerResponse
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
    controller_topic: str
    topics: "list[str]"
    retry_topic: str
    dlq_topic: str
    event_handler_map: "dict[str, Callable[[str, bytes], EventHandlerResponse]]"
    max_retries_per_event_map: "dict[str, int]"

    group_id: Optional[str] = None
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
        d = {
            "security.protocol": "SSL" if self.enable_ssl else "PLAINTEXT",
            "bootstrap.servers": self.bootstrap_servers,
            "enable.auto.commit": self.auto_commit,
            "auto.offset.reset": self.auto_offset_reset,
            "session.timeout.ms": self.session_timeout_in_ms,
        }
        if self.group_id:
            d.update(
                {
                    "group.id": self.group_id,
                }
            )

        return d


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

    def process_message(self, msg: Message):
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
            return

        handler = self._config.event_handler_map.get(event.name)
        if handler is None:
            self._handle_dlq(
                msg=msg,
                event_name=event.name,
                err_msg="Event handler not found",
            )
            return

        try:
            response = handler(key, event.payload)
            if response.success:
                LOGGER.info("msg=%s key=%s", "Processed message successfully", key)
                return

            if response.retry:
                max_retries = self._config.max_retries_per_event_map.get(event.name, 0)
                if event.retry_count > max_retries:
                    self._handle_dlq(
                        msg=msg,
                        event_name=event.name,
                        err_msg="Max retries limit reached",
                    )
                    return
                else:
                    self._handle_retry(event=event)
                    return

            if response.dlq:
                self._handle_dlq(
                    msg=msg,
                    event_name=event.name,
                    err_msg="Event handler sent for DLQ",
                    exc=response.exception,
                )
                return

        except Exception as e:
            self._handle_dlq(
                msg=msg,
                event_name=event.name,
                err_msg="Error occurred while processing event",
                exc=e,
            )


class KafkaConsumer(_KafkaConsumerHandlerMixin, Thread):
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
        topic_to_be_subscribed = (
            self._config.controller_topic
            if self.is_controller()
            else self._config.topics
        )
        self._consumer.subscribe(topic_to_be_subscribed)

    def is_controller(self) -> bool:
        return self._config.mode == ConsumerMode.CONTROLLER

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
                    if self.is_controller() and self.initial_run:
                        # Stop consumer if all configs / flags have
                        # been fetched from controller topic.
                        LOGGER.info("msg=%s", "Finished syncing with controller")
                        self.shutdown()
                else:
                    self.exception_handler(Exception(err))
                continue
            to_be_processed_messages.append(msg)

        if not to_be_processed_messages:
            return

        try:
            # Process batch using greenlet-based cooperative multitasking
            self._pool.map(self.process_message, to_be_processed_messages)
            # Wait for all greenlets to finish
            self._pool.join()
            # Commit offset in sync after current batch finishes processing
            self._consumer.commit(asynchronous=False)
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
                batch = self._fetch_batch()

                if not batch:
                    time.sleep(0.1)
                    continue

                self._exec_batch(batch=batch)
        except Exception as e:
            LOGGER.error(e, exc_info=True)
            self.exception_handler(e)

        self._consumer.close()
        LOGGER.info("msg=%s entity=%s", "Loop stopped", self.entity)
