import logging
import signal
from wsgiref.simple_server import WSGIServer, make_server

from confluent_kafka import OFFSET_END
from confluent_kafka.admin import AdminClient
from django.core.management.base import BaseCommand, CommandParser

from events_lib_py import config
from events_lib_py.healthcheck import HealthCheckUtil
from events_lib_py.server import SilentHandler, ThreadingWSGIServer

LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Startup command:
    `python manage.py start_healthcheck_server --consumer-group-id g1 --port 9200`
    """

    help = "Healthcheck server which checks status for a particular consumer group"

    def _register_signal_handlers(self):
        def handler(signum, _):
            LOGGER.info(
                "msg=%s signal=%s",
                "Stopping health check server",
                signal.Signals(signum).name,
            )
            self._server.shutdown()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGHUP, handler)

    def _generate_offset_maps(self) -> "tuple[dict, dict]":
        consumer_group_offsets = self._admin_client.list_consumer_group_offsets(
            self._consumer_group_id
        )
        topic_partitions = (
            consumer_group_offsets[self._consumer_group_id].result().topic_partitions
        )
        committed_offsets = {
            (tp.topic, tp.partition): tp.offset for tp in topic_partitions
        }

        query_map = {tp: OFFSET_END for tp in topic_partitions}
        latest_offsets = {
            (tp.topic, tp.partition): offset_result.result().offset
            for tp, offset_result in self._admin_client.list_offsets(query_map).items()
        }

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
            {"bootstrap.servers": config.CONSUMER_CONFIG["bootstrap_servers"]}
        )
        self._server: WSGIServer = make_server(
            "0.0.0.0",
            self._port,
            self._server_application,
            ThreadingWSGIServer,
            handler_class=SilentHandler,
        )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--consumer-group-id", type=str, required=True)
        parser.add_argument("--port", type=int, required=False, default=9200)

    def handle(self, *args, **kwargs):
        self._consumer_group_id, self._port = (
            kwargs["consumer_group_id"],
            kwargs["port"],
        )

        self._last_committed_offsets = {}

        self._init()
        self._register_signal_handlers()

        LOGGER.info("msg=%s port=%s", "Serving health check", self._port)
        self._server.serve_forever()
        LOGGER.info("msg=%s", "Health check server stopped")
