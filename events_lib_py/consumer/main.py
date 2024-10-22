import logging
import time

from events_lib_py.dataclasses import EventHandlerResponse

from .base import BaseKafkaConsumer
from .mixins import KafkaConsumerHealthCheckMixin, KafkaConsumerRateControlMixin

LOGGER = logging.getLogger(__name__)


class MainConsumer(
    BaseKafkaConsumer,
    KafkaConsumerHealthCheckMixin,
    KafkaConsumerRateControlMixin,
):
    def __init__(self, config):
        super().__init__(config)
        self.init_batch_counter()

    def _post_exec_batch_hook(self, results: "list[EventHandlerResponse]"):
        # Evaluate results of processed batch and slow down consumption
        # rate in case batch was identified as failed.
        if self.is_failed_batch(results=results):
            backoff = self.evaluate_exponential_backoff()
            if backoff:
                LOGGER.info(
                    "msg=%s seconds=%s",
                    "Exponential backoff triggered",
                    backoff,
                )
                time.sleep(backoff)
        elif self.batch_counter.consecutive_failed_batch_count != 0:
            self.check_if_system_recovered()
        elif self.batch_counter.consecutive_success_batch_post_recovery_count != 0:
            self.restore_batch_size()
