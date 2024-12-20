import logging
import signal

from django.core.management.base import BaseCommand, CommandParser

from events_lib_py import HealthCheckServer, KafkaConsumer, config
from events_lib_py.consumer import KafkaConsumerConfig
from events_lib_py.healthcheck import FakeHealthCheckServer

LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Startup command:
    `python manage.py start_event_consumer --group-id g1 --topics t1 t2 --retry-topic retry1 --dlq-topic dlq1`
    """

    help = "Consumes messages from Kafka topic, processes batch of events concurrently using Gevent"

    def _register_signal_handlers(
        self, consumer: KafkaConsumer, healthcheck: HealthCheckServer
    ):
        def handler(signum, _):
            LOGGER.info(
                "msg=%s signal=%s", "Received signal", signal.Signals(signum).name
            )
            consumer.shutdown()
            healthcheck.shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGHUP, handler)

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--group-id", type=str, required=True)
        parser.add_argument("--topics", type=str, required=True, nargs="+")
        parser.add_argument("--retry-topic", type=str, required=True)
        parser.add_argument("--dlq-topic", type=str, required=True)
        parser.add_argument(
            "--enable-healthcheck-server", type=bool, required=False, default=False
        )
        parser.add_argument(
            "--healthcheck-port", type=int, required=False, default=None
        )
        parser.add_argument("--metrics-port", type=int, required=False, default=None)

    def _build_consumer_config(self, kwargs: dict) -> KafkaConsumerConfig:
        group_id, topics, retry_topic, dlq_topic = (
            kwargs["group_id"],
            kwargs["topics"],
            kwargs["retry_topic"],
            kwargs["dlq_topic"],
        )

        if not group_id:
            raise Exception("Consumer Group ID needed")
        if not topics:
            raise Exception(
                "Atleast one valid topic needed for kafka consumer to start"
            )
        if not retry_topic:
            raise Exception("Valid `retry-topic` needed")
        if not dlq_topic:
            raise Exception("Valid `dlq-topic` needed")

        return KafkaConsumerConfig(
            group_id=group_id,
            topics=topics,
            retry_topic=retry_topic,
            dlq_topic=dlq_topic,
            **config.CONSUMER_CONFIG,
        )

    def handle(self, *args, **kwargs):
        if hook := config.PRE_INIT_HOOK:
            metrics_port = kwargs.get("metrics_port")
            hook(metrics_port)

        consumer = KafkaConsumer(config=self._build_consumer_config(kwargs))

        enable_healthcheck_server = kwargs["enable_healthcheck_server"]
        healthcheck_server_class = (
            HealthCheckServer if enable_healthcheck_server else FakeHealthCheckServer
        )
        healthcheck = healthcheck_server_class(
            port=kwargs.get("healthcheck_port") or config.HEALTHCHECK_PORT,
            is_healthy=consumer.is_healthy,
        )

        self._register_signal_handlers(consumer=consumer, healthcheck=healthcheck)

        healthcheck.start()
        consumer.start()

        # Main thread will stay blocked until consumer loop finishes
        consumer.join()
        healthcheck.join()
