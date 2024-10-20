import logging
import uuid
from typing import Any, Callable, Optional

from confluent_kafka import Message

from .config import IS_TEST_ENV, PRODUCER_CONFIG
from .consumer import KafkaConsumer, KafkaConsumerConfig
from .dataclasses import EventHandlerResponse
from .healthcheck import HealthCheckServer
from .metrics import (
    KAFKA_MESSAGE_QUEUED_TOTAL,
    KAFKA_MESSAGE_SENT_TOTAL,
)
from .pb.event_pb2 import Event
from .producer import FakeKafkaProducer, KafkaProducer, KafkaProducerConfig

__all__ = (
    "EventHandlerResponse",
    "HealthCheckServer",
    "KafkaConsumer",
    "KafkaConsumerConfig",
    "KafkaProducer",
    "KafkaProducerConfig",
    "send_event",
    "send_raw_message",
)


default_producer: "Optional[KafkaProducer | FakeKafkaProducer]" = None


LOGGER = logging.getLogger(__name__)


def init_producer():
    LOGGER.info("msg=%s", "Starting KafkaProducer")

    global default_producer
    producer_class = KafkaProducer
    if IS_TEST_ENV:
        producer_class = FakeKafkaProducer

    default_producer = producer_class(config=KafkaProducerConfig(**PRODUCER_CONFIG))


def generate_sent_callback(
    topic: str, event_id: str, event_name: Optional[str]
) -> Callable:
    def callback(error: Any, _):
        if error is None:
            KAFKA_MESSAGE_SENT_TOTAL.labels(topic=topic, event_name=event_name).inc()
            msg, logger = "Message sent", LOGGER.info
        else:
            msg, logger = f"Failed to push message, exc: {error}", LOGGER.warning
        logger(
            "msg=%s topic=%s id=%s name=%s",
            msg,
            topic,
            event_id,
            event_name,
        )

    return callback


def generate_queued_callback(
    topic: str, event_id: str, event_name: Optional[str]
) -> Callable:
    def callback():
        LOGGER.info(
            "msg=%s topic=%s id=%s name=%s",
            "Message queued",
            topic,
            event_id,
            event_name,
        )
        KAFKA_MESSAGE_QUEUED_TOTAL.labels(topic=topic, event_name=event_name).inc()

    return callback


def send_event(
    topic: str,
    name: str,
    payload: bytes,
    retry_count: int = 0,
    valid_till: Optional[str] = None,
):
    global default_producer
    if default_producer is None:
        init_producer()

    event_id = str(uuid.uuid4())
    event = Event(
        name=name,
        payload=payload,
        retry_count=retry_count,
        valid_till=valid_till,
    )
    default_producer.send_message(
        topic=topic,
        id=event_id.encode(),
        message=event.SerializeToString(),
        queued_callback=generate_queued_callback(
            topic=topic, event_id=event_id, event_name=event.name
        ),
        sent_callback=generate_sent_callback(
            topic=topic, event_id=event_id, event_name=event.name
        ),
    )


def send_to_dlq(
    dlq_topic: str,
    msg: Message,
    event_name: Optional[str] = None,
):
    global default_producer
    if default_producer is None:
        init_producer()

    id = msg.key()
    default_producer.send_message(
        topic=dlq_topic,
        id=id,
        message=msg.value(),
        queued_callback=generate_queued_callback(
            topic=dlq_topic, event_id=id.decode(), event_name=event_name
        ),
        sent_callback=generate_sent_callback(
            topic=dlq_topic, event_id=id.decode(), event_name=event_name
        ),
    )


def send_raw_message(
    topic: str,
    key: bytes,
    value: bytes,
    headers: Optional[dict] = None,
    queued_callback: Optional[Callable] = None,
    sent_callback: Optional[Callable] = None,
):
    global default_producer
    if default_producer is None:
        init_producer()

    default_producer.send_message(
        topic=topic,
        key=key,
        value=value,
        headers=headers,
        queued_callback=queued_callback,
        sent_callback=sent_callback,
    )
