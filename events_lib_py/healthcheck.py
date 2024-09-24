import logging
from threading import Thread
from typing import Callable
from wsgiref.simple_server import WSGIServer, make_server

from events_lib_py.server import SilentHandler, ThreadingWSGIServer

LOGGER = logging.getLogger(__name__)


class HealthCheckUtil:
    @staticmethod
    def is_consumer_healthy(
        prev_committed_offsets: "dict[tuple[str, int], int]",
        latest_offsets: "dict[tuple[str, int], int]",
        committed_offsets: "dict[tuple[str, int], int]",
    ):
        if not prev_committed_offsets:
            return False

        healthy = True
        for key in committed_offsets.keys():
            (
                prev_committed_offset,
                current_committed_offset,
                current_latest_offset,
            ) = (
                prev_committed_offsets.get(key),
                committed_offsets[key],
                latest_offsets[key],
            )

            if not prev_committed_offset:
                # Consumer has been reassigned a new partition due to rebalancing
                healthy = False
                break

            if current_committed_offset == current_latest_offset:
                continue

            if current_committed_offset == prev_committed_offset:
                healthy = False
                break

        return healthy


class HealthCheckServer(Thread):
    def __init__(self, port: int, is_healthy: Callable[[], bool]):
        super().__init__()

        self._is_healthy = is_healthy
        self._port: int = port
        self._server: WSGIServer = make_server(
            "0.0.0.0",
            self._port,
            self._application,
            ThreadingWSGIServer,
            handler_class=SilentHandler,
        )

    def _application(self, environ, start_response):
        path = environ.get("PATH_INFO", "")

        if path == "/health":
            status = "200 OK"
            headers = [("Content-type", "application/json")]
            start_response(status, headers)

            response = (
                '{"status": "healthy"}'
                if self._is_healthy()
                else '{"status": "unhealthy"}'
            )
            return [response.encode("utf-8")]

        # Handle 404 Not Found
        status = "404 Not Found"
        headers = [("Content-type", "text/plain")]
        start_response(status, headers)
        return [b"Not Found"]

    def run(self):
        LOGGER.info("msg=%s port=%s", "Serving health check", self._port)
        self._server.serve_forever()
        LOGGER.info("msg=%s", "Health check server stopped")

    def shutdown(self):
        LOGGER.info("msg=%s", "Stopping health check server")
        self._server.shutdown()
