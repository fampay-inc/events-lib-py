from prometheus_client import Counter, Histogram

KAFKA_MESSAGE_QUEUED_TOTAL = Counter(
    name="kafka_message_queued_total",
    documentation="Total Kafka messages queued",
    labelnames=["topic", "event_name"],
)

KAFKA_MESSAGE_SENT_TOTAL = Counter(
    name="kafka_message_sent_total",
    documentation="Total Kafka messages sent to broker",
    labelnames=["topic", "event_name"],
)

KAFKA_MESSAGE_SENT_TO_DLQ_TOTAL = Counter(
    name="kafka_message_sent_to_dlq_total",
    documentation="Total Kafka messages sent to DLQ topic",
)

KAFKA_CONSUMER_BATCH_FETCH_LATENCY = Histogram(
    name="kafka_consumer_batch_fetch_latency",
    documentation="Kafka consumer batch fetching latency (s)",
)

KAFKA_CONSUMER_BATCH_PROCESSING_LATENCY = Histogram(
    name="kafka_consumer_batch_processing_latency",
    documentation="Kafka consumer batch processing latency (s)",
)

KAFKA_CONTROLLER_FLAG_SWITCH_COUNT_TOTAL = Counter(
    name="kafka_controller_flag_switch_count_total",
    documentation="Signifies number of times the controller flag has been toggled",
    labelnames=["flag_name", "flag_value"],
)
