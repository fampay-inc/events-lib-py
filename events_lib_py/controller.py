from typing import Callable
from events_lib_py import config
from events_lib_py.consumer import LOGGER, KafkaConsumer
from events_lib_py.dataclasses import EventHandlerResponse


from confluent_kafka import Consumer


class KafkaConsumerController(KafkaConsumer):
    entity = "controller"
    gevent_pool_size = 100
    attr_dataclass_map = {
        "config": config.CONSUMER_CONTROLLER_CONFIG,
        "flag": config.CONSUMER_CONTROLLER_CONFIG,
    }

    def __init__(
        self, config, attr_apply_handlers: dict[str, Callable[[int], None]] = {}
    ):
        super().__init__(config)
        self.attr_apply_handlers = attr_apply_handlers
        self.initial_run = True

    def process_message(self, msg) -> EventHandlerResponse:
        """
        Processes change attribute event from controller
        topic and updates attribute value.
        """
        key, value = msg.key().decode(), int(msg.value())
        LOGGER.info("msg=%s key=%s value=%s", "Received controller event", key, value)
        attr_type, attr_name = key.split(":")
        self.attr_dataclass_map[attr_type].update(attr_name=attr_name, value=value)
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
