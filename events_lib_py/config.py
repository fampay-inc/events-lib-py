from typing import Callable, Optional
from django.conf import settings


if not hasattr(settings, "EVENTS_LIB_PY_CONFIG"):
    raise AttributeError("Provide valid `EVENTS_LIB_PY_CONFIG` in project settings")


IS_TEST_ENV: bool = settings["EVENTS_LIB_PY_CONFIG"]["IS_TEST_ENV"]
PRE_INIT_HOOK: Optional[Callable] = settings["EVENTS_LIB_PY_CONFIG"].get(
    "PRE_INIT_HOOK"
)
CONSUMER_CONFIG: dict = settings["EVENTS_LIB_PY_CONFIG"]["CONSUMER_CONFIG"]
PRODUCER_CONFIG: dict = settings["EVENTS_LIB_PY_CONFIG"]["PRODUCER_CONFIG"]
HEALTHCHECK_PORT: int = settings["EVENTS_LIB_PY_CONFIG"].get(
    "HEALTHCHECK_PORT", 9101
)
