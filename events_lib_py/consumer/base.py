import logging
import time
from dataclasses import dataclass
from threading import Thread
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition
from gevent.pool import Pool

from events_lib_py.dataclasses import EventHandlerResponse
from events_lib_py.metrics import (
    KAFKA_CONSUMER_BATCH_FETCH_LATENCY,
    KAFKA_CONSUMER_BATCH_PROCESSING_LATENCY,
)

from .mixins import KafkaConsumerHandlerMixin

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


class BaseKafkaConsumer(KafkaConsumerHandlerMixin, Thread):
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
        def on_assign(_: Consumer, partitions: "list[TopicPartition]"):
            LOGGER.info("msg=%s partitions=%s", "Assigned partitions", partitions)
            # Resetting previous committed offsets since
            # another partition can be reassigned to the
            # consumer.
            self._prev_committed_offsets = {}

        self._consumer.subscribe(self._config.topics, on_assign=on_assign)

    def _handle_partition_end_reached(self):
        """
        Should comprise of code that is to be executed when
        committed offset of consumer is the same as latest message
        offset of the assigned partition.
        """
        pass

    def _post_exec_batch_hook(self, results: "list[EventHandlerResponse]"):
        """
        Code that is to be executed after processing each batch
        can be put here.
        """
        pass

    def _skip_iteration(self) -> bool:
        """
        Decides if consumer loop iteration is to be executed.
        """
        return False

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
        except Exception as e:
            LOGGER.error(
                "msg=%s exc=%s",
                "Exception occurred during batch processing",
                e,
                exc_info=True,
            )
            self.exception_handler(e)
        else:
            self._post_exec_batch_hook(results=results)

    def run(self):
        LOGGER.info("msg=%s entity=%s", "Starting loop", self.entity)

        try:
            while self._keep_running:
                if self._skip_iteration():
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
