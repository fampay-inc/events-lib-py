from dataclasses import dataclass


@dataclass
class EventHandlerResponse:
    success: bool = False
    retry: bool = False
    dlq: bool = False
    exception: Exception = None
