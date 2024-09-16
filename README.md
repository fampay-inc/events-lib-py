# events-lib-py

## Configure
Put the below configuration in `settings.py` of your Django project
```py
from events_lib_py import KafkaConsumerConfig, KafkaProducerConfig

EVENTS_LIB_PY_CONFIG = {
    "IS_TEST_ENV": False,
    "PRE_INIT_HOOK": lambda: """DO SOMETHING""",
    "CONSUMER_CONFIG": {...},
    "PRODUCER_CONFIG": {...},
    "HEALTHCHECK_PORT": 9101 # default
}
```
