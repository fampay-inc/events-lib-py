from typing import Any, Callable, Optional
from django.conf import settings
from django.utils.module_loading import import_string


DEFAULTS = {
    "IS_TEST_ENV": False,
    "PRE_INIT_HOOK": None,
    "CONSUMER_CONFIG": {
        "bootstrap_servers": "127.0.0.1:9092",
        "enable_ssl": False,
        "auto_commit": False,
        "event_handler_map": {},
        "max_retries_per_event_map": {},
        "dlq_pre_send_hook": None,
        "generic_exception_handler": None,
    },
    "PRODUCER_CONFIG": {
        "bootstrap_servers": "127.0.0.1:9092",
        "enable_ssl": False,
    },
    "HEALTHCHECK_PORT": 9101,
}


def get_or_default(key: str) -> Any:
    default = DEFAULTS[key]
    return settings.EVENTS_LIB_PY.get(
        key, default.copy() if isinstance(default, dict) else default
    )


def import_from_string(path: str):
    """
    Attempt to import from a string representation.
    """
    try:
        return import_string(path)
    except ImportError:
        raise
        # raise ImportError(f"Could not import '{path}'")


def load_consumer_config() -> dict:
    config: dict = DEFAULTS["CONSUMER_CONFIG"].copy()
    if "CONSUMER_CONFIG" not in settings.EVENTS_LIB_PY:
        return config

    config.update(settings.EVENTS_LIB_PY["CONSUMER_CONFIG"])

    importable_props = [
        "event_handler_map",
        "max_retries_per_event_map",
        "dlq_pre_send_hook",
        "generic_exception_handler",
    ]
    for prop in importable_props:
        if (path := config.get(prop)) and isinstance(path, str):
            config[prop] = import_from_string(path)

    return config


def load_producer_config() -> dict:
    config: dict = DEFAULTS["PRODUCER_CONFIG"].copy()
    if "PRODUCER_CONFIG" not in settings.EVENTS_LIB_PY:
        return config

    config.update(settings.EVENTS_LIB_PY["PRODUCER_CONFIG"])

    return config


IS_TEST_ENV: bool = get_or_default("IS_TEST_ENV")

PRE_INIT_HOOK: "Optional[str | Callable]" = get_or_default("PRE_INIT_HOOK")
if PRE_INIT_HOOK and isinstance(PRE_INIT_HOOK, str):
    PRE_INIT_HOOK = import_from_string(PRE_INIT_HOOK)

PRODUCER_CONFIG: dict = load_producer_config()
CONSUMER_CONFIG: dict = load_consumer_config()

HEALTHCHECK_PORT: int = get_or_default("HEALTHCHECK_PORT")
