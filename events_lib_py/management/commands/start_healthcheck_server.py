import logging
import signal
from threading import Thread
from wsgiref.simple_server import WSGIServer, make_server

from confluent_kafka import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, OffsetSpec
from django.core.management.base import BaseCommand, CommandParser

from events_lib_py import config
from events_lib_py.healthcheck import HealthCheckUtil
from events_lib_py.server import SilentHandler, ThreadingWSGIServer

LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Startup command:
    `python manage.py start_healthcheck_server --consumer-group-id g1 g2 --port 9200`
    """

    help = "Healthcheck server which checks status for a particular consumer group"

    def _register_signal_handlers(self):
        def handler(signum, _):
            LOGGER.info(
                "msg=%s signal=%s",
                "Stopping health check server",
                signal.Signals(signum).name,
            )
            # FIXME: gevent.exceptions.BlockingSwitchOutError: Impossible to call blocking function in the event loop callback
            self._server.shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGHUP, handler)

    def _generate_offset_maps(self) -> "tuple[dict, dict]":
        latest_offsets, committed_offsets = {}, {}
        for id in self._consumer_group_ids:
            consumer_group_offsets = self._admin_client.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id=id)]
            )
            topic_partitions = (
                consumer_group_offsets[id].result(timeout=0.2).topic_partitions
            )
            committed_offsets.update(
                {(tp.topic, tp.partition): tp.offset for tp in topic_partitions}
            )

            query_map = {tp: OffsetSpec.latest() for tp in topic_partitions}
            latest_offsets.update(
                {
                    (tp.topic, tp.partition): offset_result.result(timeout=0.2).offset
                    for tp, offset_result in self._admin_client.list_offsets(
                        query_map
                    ).items()
                }
            )

        return latest_offsets, committed_offsets

    def _is_consumer_group_healthy(self) -> bool:
        latest_offsets, committed_offsets = self._generate_offset_maps()
        healthy = HealthCheckUtil.is_consumer_healthy(
            prev_committed_offsets=self._last_committed_offsets,
            latest_offsets=latest_offsets,
            committed_offsets=committed_offsets,
        )
        self._last_committed_offsets = committed_offsets
        return healthy

    def _server_application(self, environ, start_response):
        path = environ.get("PATH_INFO", "")

        if path == "/health":
            status = "200 OK"
            headers = [("Content-type", "application/json")]
            start_response(status, headers)

            response = (
                '{"status": "healthy"}'
                if self._is_consumer_group_healthy()
                else '{"status": "unhealthy"}'
            )
            return [response.encode("utf-8")]

        # Handle 404 Not Found
        status = "404 Not Found"
        headers = [("Content-type", "text/plain")]
        start_response(status, headers)
        return [b"Not Found"]

    def _init(self):
        self._admin_client = AdminClient(
            {
                "bootstrap.servers": config.CONSUMER_CONFIG["bootstrap_servers"],
                "security.protocol": "SSL"
                if config.CONSUMER_CONFIG["enable_ssl"]
                else "PLAINTEXT",
            }
        )
        self._server: WSGIServer = make_server(
            "0.0.0.0",
            self._port,
            self._server_application,
            ThreadingWSGIServer,
            handler_class=SilentHandler,
        )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--consumer-group-ids", type=str, required=True, nargs="+")
        parser.add_argument("--port", type=int, required=False, default=9200)

    def handle(self, *args, **kwargs):
        self._consumer_group_ids, self._port = (
            kwargs["consumer_group_ids"],
            kwargs["port"],
        )

        self._last_committed_offsets = {}

        self._init()
        self._register_signal_handlers()

        LOGGER.info("msg=%s port=%s", "Serving health check", self._port)
        thread = Thread(target=self._server.serve_forever)
        thread.start()
        thread.join()
        LOGGER.info("msg=%s", "Health check server stopped")
