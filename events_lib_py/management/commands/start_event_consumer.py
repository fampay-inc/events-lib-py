import logging
import signal

from django.core.management.base import BaseCommand, CommandParser

from events_lib_py import HealthCheckServer, KafkaConsumer, config
from events_lib_py.controller import KafkaConsumerController
from events_lib_py.constants import ConsumerMode
from events_lib_py.consumer import KafkaConsumerConfig
from events_lib_py.healthcheck import FakeHealthCheckServer

LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Startup command:
    `
    python manage.py start_event_consumer \
        --mode main \
        --group-id g1 \
        --topics t1 t2 \
        --controller-topic controller1 \
        --retry-topic retry1 \
        --dlq-topic dlq1 \
        [--enable-healthcheck-server true] \
        [--healthcheck-port 9010] \
        [--metrics-port 9001]
    `
    """

    help = "Consumes messages from Kafka topic, processes batch of events concurrently using Gevent"

    def _register_signal_handlers(
        self,
        consumer: KafkaConsumer,
        controller: KafkaConsumerController,
        healthcheck: HealthCheckServer,
    ):
        def handler(signum, _):
            LOGGER.info(
                "msg=%s signal=%s", "Received signal", signal.Signals(signum).name
            )
            consumer.shutdown()
            controller.shutdown()
            healthcheck.shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGHUP, handler)

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--mode", type=str, required=False, default=ConsumerMode.MAIN
        )
        parser.add_argument("--group-id", type=str, required=True)
        parser.add_argument("--topics", type=str, required=True, nargs="+")
        parser.add_argument("--controller-topic", type=str, required=True)
        parser.add_argument("--retry-topic", type=str, required=True)
        parser.add_argument("--dlq-topic", type=str, required=True)
        parser.add_argument(
            "--enable-healthcheck-server", type=bool, required=False, default=False
        )
        parser.add_argument(
            "--healthcheck-port", type=int, required=False, default=None
        )
        parser.add_argument("--metrics-port", type=int, required=False, default=None)

    def _build_consumer_configs(
        self, kwargs: dict
    ) -> "tuple[KafkaConsumerConfig, KafkaConsumerConfig]":
        """
        Builds `KafkaConsumerConfig` instaces for both controller and consumer.
        Returns `(controller_config, consumer_config)`.
        """
        mode, group_id, controller_topic, topics, retry_topic, dlq_topic = (
            kwargs["mode"],
            kwargs["group_id"],
            kwargs["controller_topic"],
            kwargs["topics"],
            kwargs["retry_topic"],
            kwargs["dlq_topic"],
        )

        if not group_id:
            raise Exception("Consumer Group ID needed")
        if not controller_topic:
            raise Exception("Valid `controller-topic` needed")
        if not topics:
            raise Exception(
                "Atleast one valid topic needed for kafka consumer to start"
            )
        if not retry_topic:
            raise Exception("Valid `retry-topic` needed")
        if not dlq_topic:
            raise Exception("Valid `dlq-topic` needed")

        config_dict = {
            "controller_topic": controller_topic,
            "topics": topics,
            "retry_topic": retry_topic,
            "dlq_topic": dlq_topic,
            "batch_size": config.CONSUMER_CONTROLLER_CONFIG.batch_size,
            **config.CONSUMER_CONFIG,
        }

        return (
            KafkaConsumerConfig(
                mode=ConsumerMode.CONTROLLER,
                # FIXME: Solve for multiple consumer instances with same group id
                group_id=f"{group_id}.controller",
                **config_dict,
            ),
            KafkaConsumerConfig(mode=mode, group_id=group_id, **config_dict),
        )

    def handle(self, *args, **kwargs):
        if hook := config.PRE_INIT_HOOK:
            metrics_port = kwargs.get("metrics_port")
            hook(metrics_port)

        controller_config, consumer_config = self._build_consumer_configs(kwargs)

        controller = KafkaConsumerController(
            config=controller_config,
            attr_apply_handlers={
                "config:batch_size": lambda val: setattr(
                    consumer_config, "batch_size", val
                )
            },
        )
        controller.sync()

        consumer = KafkaConsumer(config=consumer_config)

        enable_healthcheck_server = kwargs["enable_healthcheck_server"]
        healthcheck_server_class = (
            HealthCheckServer if enable_healthcheck_server else FakeHealthCheckServer
        )
        healthcheck = healthcheck_server_class(
            port=kwargs.get("healthcheck_port") or config.HEALTHCHECK_PORT,
            is_healthy=consumer.is_healthy,
        )

        self._register_signal_handlers(
            consumer=consumer, controller=controller, healthcheck=healthcheck
        )

        healthcheck.start()
        controller.start()
        consumer.start()

        # Main thread will stay blocked until all threads shutdown gracefully
        consumer.join()
        controller.join()
        healthcheck.join()
