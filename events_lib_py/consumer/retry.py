import logging
import time

from events_lib_py import config
from events_lib_py.constants import ControllerFlagName

from .base import BaseKafkaConsumer
from .mixins import KafkaConsumerHealthCheckMixin, KafkaConsumerRateControlMixin

LOGGER = logging.getLogger(__name__)


class RetryConsumer(
    BaseKafkaConsumer,
    KafkaConsumerHealthCheckMixin,
    KafkaConsumerRateControlMixin,
):
    def _handle_partition_end_reached(self):
        # Sending notification to controller topic to disable retry consumer
        self._notify_controller(ControllerFlagName.retry_consumer_enabled, 0)

    def _skip_iteration(self):
        if config.CONTROLLER_FLAG.retry_consumer_enabled == 0:
            # Keep retry consumer blocked until it's enabled
            time.sleep(5)
            return True
        return False
