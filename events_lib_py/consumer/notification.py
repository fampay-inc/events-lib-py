import logging
from typing import Any, Callable

from events_lib_py.metrics import KAFKA_CONTROLLER_FLAG_SWITCH_COUNT_TOTAL

LOGGER = logging.getLogger(__name__)


class ControllerFlagNotification:
    def __init__(self, controller_topic: str, flag_name: str, flag_value: int):
        self.controller_topic = controller_topic
        self.flag_name = flag_name
        self.flag_value = flag_value

    def _generate_notification_queued_callback(self) -> Callable:
        def callback():
            LOGGER.info(
                "msg=%s name=%s value=%s",
                "Flag notification queued",
                self.flag_name,
                self.flag_value,
            )

        return callback

    def _generate_notification_sent_callback(self):
        def callback(error: Any, _):
            if error is None:
                KAFKA_CONTROLLER_FLAG_SWITCH_COUNT_TOTAL.labels(
                    flag_name=self.flag_name,
                    flag_value=self.flag_value,
                ).inc()
                msg, logger = "Flag notification sent", LOGGER.info
            else:
                msg, logger = (
                    f"Failed to send flag notification, exc: {error}",
                    LOGGER.warning,
                )
            logger("msg=%s name=%s value=%s", msg, self.flag_name, self.flag_value)

        return callback

    def notify_controller(self):
        from events_lib_py import send_raw_message

        send_raw_message(
            topic=self.controller_topic,
            key=f"flag:{self.flag_name}".encode(),
            value=f"{self.flag_value}".encode(),
            queued_callback=self._generate_notification_queued_callback(),
            sent_callback=self._generate_notification_sent_callback(),
        )
