class ConsumerMode:
    MAIN = "main"
    CONTROLLER = "controller"
    RETRY = "retry"
    DLQ = "dlq"


class KafkaConsumerControllerFlagName:
    retry_consumer_enabled = "retry_consumer_enabled"
