from dataclasses import dataclass, field
from multiprocessing import Value
from multiprocessing.sharedctypes import Synchronized


@dataclass
class EventHandlerResponse:
    success: bool = False
    retry: bool = False
    dlq: bool = False
    exception: Exception = None


@dataclass
class KafkaConsumerControllerConfig:
    _batch_size: "Synchronized[int]" = field(default_factory=lambda: Value("i", 10))
    _batch_failure_event_percentage: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _throttle_after_failed_batch_threshold: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _reset_throttle_after_success_batch_threshold: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_exponential_backoff_enabled: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_exponential_backoff_initial_delay: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_exponential_backoff_coefficient: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_exponential_backoff_max_delay: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_batch_size_slice_percentage: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_min_batch_size: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )
    _retry_batch_size_restore_percentage: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )

    @property
    def batch_size(self) -> int:
        with self._batch_size.get_lock():
            return self._batch_size.value

    @property
    def batch_failure_event_percentage(self) -> int:
        with self._batch_failure_event_percentage.get_lock():
            return self._batch_failure_event_percentage.value

    @property
    def throttle_after_failed_batch_threshold(self) -> int:
        with self._throttle_after_failed_batch_threshold.get_lock():
            return self._throttle_after_failed_batch_threshold.value

    @property
    def reset_throttle_after_success_batch_threshold(self) -> int:
        with self._reset_throttle_after_success_batch_threshold.get_lock():
            return self._reset_throttle_after_success_batch_threshold.value

    @property
    def retry_exponential_backoff_enabled(self) -> int:
        with self._retry_exponential_backoff_enabled.get_lock():
            return self._retry_exponential_backoff_enabled.value

    @property
    def retry_exponential_backoff_initial_delay(self) -> int:
        with self._retry_exponential_backoff_initial_delay.get_lock():
            return self._retry_exponential_backoff_initial_delay.value

    @property
    def retry_exponential_backoff_coefficient(self) -> int:
        with self._retry_exponential_backoff_coefficient.get_lock():
            return self._retry_exponential_backoff_coefficient.value

    @property
    def retry_exponential_backoff_max_delay(self) -> int:
        with self._retry_exponential_backoff_max_delay.get_lock():
            return self._retry_exponential_backoff_max_delay.value

    @property
    def retry_batch_size_slice_percentage(self) -> int:
        with self._retry_batch_size_slice_percentage.get_lock():
            return self._retry_batch_size_slice_percentage.value

    @property
    def retry_min_batch_size(self) -> int:
        with self._retry_min_batch_size.get_lock():
            return self._retry_min_batch_size.value

    @property
    def retry_batch_size_restore_percentage(self) -> int:
        with self._retry_batch_size_restore_percentage.get_lock():
            return self._retry_batch_size_restore_percentage.value

    def update(self, attr_name: str, value: int):
        attr: "Synchronized[int]" = getattr(self, f"_{attr_name}")
        with attr.get_lock():
            attr.value = value


@dataclass
class KafkaConsumerControllerFlag:
    _retry_consumer_enabled: "Synchronized[int]" = field(
        default_factory=lambda: Value("i", 0)
    )

    @property
    def retry_consumer_enabled(self) -> int:
        with self._retry_consumer_enabled.get_lock():
            return self._retry_consumer_enabled.value

    def update(self, attr_name: str, value: int):
        attr: "Synchronized[int]" = getattr(self, f"_{attr_name}")
        with attr.get_lock():
            attr.value = value
