import atexit
import logging
import socket
import time
from dataclasses import dataclass
from threading import Thread
from typing import Callable, Optional

from confluent_kafka import Producer

LOGGER = logging.getLogger(__name__)


@dataclass
class KafkaProducerConfig:
    bootstrap_servers: str = "127.0.0.1:9092"
    enable_ssl: bool = True
    acks: str = "all"
    client_id: str = socket.gethostname()
    flush_interval = 500  # Default flush interval in milliseconds
    flush_batch_size = 32 * 1024  # Default maximum batch size (16KB)
    block_timeout = 10_000  # in ms
    ack_event_timeout = 10_000  # in ms
    max_buffer_memory = 32 * 1024 * 1024  # size in bytes

    def to_confluent_config(self) -> dict:
        return {
            "security.protocol": "SSL" if self.enable_ssl else "PLAINTEXT",
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "client.id": self.client_id,
            "linger.ms": self.flush_interval,
            "batch.size": self.flush_batch_size,
            "request.timeout.ms": self.block_timeout,
            "delivery.timeout.ms": self.ack_event_timeout,
        }


class DeliveryReportConsumer(Thread):
    def __init__(self, producer: Producer):
        super().__init__()
        self._producer = producer
        self._running = True

    def pause(self):
        LOGGER.info("msg=%s", "DeliveryReportConsumer loop stopped")
        self._running = False

    def run(self):
        LOGGER.info("msg=%s", "DeliveryReportConsumer loop started")
        while self._running or len(self._producer):
            self._producer.poll(0)
            time.sleep(0.01)
        LOGGER.info("msg=%s", "DeliveryReportConsumer loop exited")


class KafkaProducer:
    def __init__(self, config: KafkaProducerConfig):
        self._config = config
        self._producer = Producer(config.to_confluent_config())

        self._drc = DeliveryReportConsumer(producer=self._producer)
        self._drc.start()

        atexit.register(self.join)

    def join(self):
        LOGGER.info("msg=%s", "Exiting KafkaProducer")
        self._drc.pause()
        self._drc.join()
        LOGGER.info("msg=%s", "KafkaProducer closed")

    def send_message(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        headers: Optional[dict] = None,
        queued_callback: Optional[Callable] = None,
        sent_callback: Optional[Callable] = None,
    ) -> None:
        self._producer.poll(0)
        self._producer.produce(
            topic=topic,
            key=key,
            value=value,
            callback=sent_callback,
            headers=headers,
        )

        if callable(queued_callback):
            queued_callback()


class FakeKafkaProducer:
    def __init__(self, *_, **__):
        pass

    def send_message(self, *_, **__):
        pass
