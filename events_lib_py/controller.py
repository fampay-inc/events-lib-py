from typing import Any, Callable

from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

from events_lib_py import config
from events_lib_py.consumer import LOGGER, KafkaConsumer
from events_lib_py.dataclasses import EventHandlerResponse
from events_lib_py.metrics import KAFKA_CONTROLLER_FLAG_SWITCH_COUNT_TOTAL


class KafkaConsumerController(KafkaConsumer):
    entity = "controller"
    gevent_pool_size = 100
    attr_dataclass_map = {
        "config": config.CONSUMER_CONTROLLER_CONFIG,
        "flag": config.CONSUMER_CONTROLLER_FLAG,
    }

    def __init__(
        self, config, attr_apply_handlers: dict[str, Callable[[int], None]] = {}
    ):
        super().__init__(config)
        self.attr_apply_handlers = attr_apply_handlers
        self.initial_run = True

    def _subscribe_topic(self):
        partitions = [
            TopicPartition(
                topic=self._config.controller_topic,
                partition=0,
                offset=OFFSET_BEGINNING,
            )
        ]
        self._consumer.assign(partitions)
        init_msg = self._consumer.poll(10)

        err = None
        if init_msg is None:
            err = "Partition assignment timed out"
        elif init_msg.error():
            err = f"Error occurred during partition assignment: {init_msg.error()}"

        if err:
            raise Exception(err)

    def _handle_partition_end_reached(self):
        super()._handle_partition_end_reached()
        if self.initial_run:
            # Stop consumer if all configs / flags have
            # been fetched from controller topic.
            LOGGER.info("msg=%s", "Finished syncing with controller")
            self.shutdown()

    def process_message(self, msg) -> EventHandlerResponse:
        """
        Processes change attribute event from controller
        topic and updates attribute value.
        """
        key, value = msg.key().decode(), int(msg.value())
        LOGGER.info("msg=%s key=%s value=%s", "Received controller event", key, value)
        attr_type, attr_name = key.split(":")
        self.attr_dataclass_map[attr_type].setattr(name=attr_name, value=value)
        if handler := self.attr_apply_handlers.get(key):
            handler(value)

        return EventHandlerResponse(success=True)

    def _reset(self):
        """
        Resets self's state after initial sync so that same instance of
        `KafkaConsumerController` can be run in a thread to keep listening
        events from controller topic.
        """
        LOGGER.info("msg=%s", "Resetting controller")
        self._consumer = Consumer(self._config.to_confluent_config())
        self._check_broker_connection()
        self._subscribe_topic()
        self._keep_running = True
        self.initial_run = False
        LOGGER.info("msg=%s", "Controller ready for thread execution")

    def sync(self):
        """
        Executes KafkaConsumer's `run` method in the main thread.
        Syncs config from the controller topic before starting topic consumer.
        """
        self.run()
        self._reset()


class KafkaConsumerControllerFlagNotification:
    def __init__(self, flag_name: str, flag_value: int):
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
        from . import send_raw_message

        send_raw_message(
            topic=self._config.controller_topic,
            key=f"flag:{self.flag_name}".encode(),
            value=f"{self.flag_value}".encode(),
            queued_callback=self._generate_notification_queued_callback(),
            sent_callback=self._generate_notification_sent_callback(),
        )
