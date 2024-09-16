import logging
from socketserver import ThreadingMixIn
from threading import Thread
from typing import Optional
from wsgiref.simple_server import WSGIServer, make_server

LOGGER = logging.getLogger(__name__)


class HealthCheckServer(Thread):
    class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
        """Thread per request HTTP server."""

        # Make worker threads "fire and forget". Beginning with Python 3.7 this
        # prevents a memory leak because ``ThreadingMixIn`` starts to gather all
        # non-daemon threads in a list in order to join on them at server close.
        daemon_threads = True

    def __init__(self, port: int):
        super().__init__()

        self._is_healthy: bool = False
        self._port: int = port
        self._server: Optional[WSGIServer] = make_server(
            "0.0.0.0", self._port, self._application, self.ThreadingWSGIServer
        )

    def _application(self, environ, start_response):
        path = environ.get("PATH_INFO", "")

        if path == "/health":
            status = "200 OK"
            headers = [("Content-type", "application/json")]
            start_response(status, headers)

            response = (
                '{"status": "healthy"}'
                if self._is_healthy
                else '{"status": "unhealthy"}'
            )
            return [response.encode("utf-8")]

        # Handle 404 Not Found
        status = "404 Not Found"
        headers = [("Content-type", "text/plain")]
        start_response(status, headers)
        return [b"Not Found"]

    def _update_health_status(self, is_healthy: bool) -> None:
        self._is_healthy = is_healthy

    def healthy(self):
        self._update_health_status(is_healthy=True)

    def unhealthy(self):
        self._update_health_status(is_healthy=False)

    def run(self):
        LOGGER.info("msg=%s port=%s", "Serving health check", self._port)
        self._server.serve_forever()
        LOGGER.info("msg=%s", "Health check server stopped")

    def shutdown(self):
        LOGGER.info("msg=%s", "Stopping health check server")
        self._server.shutdown()
