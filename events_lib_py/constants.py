class ConsumerMode:
    MAIN = "main"
    CONTROLLER = "controller"
    RETRY = "retry"
    DLQ = "dlq"


class ControllerFlagName:
    retry_consumer_enabled = "retry_consumer_enabled"
    system_in_degraded_state = "system_in_degraded_state"
