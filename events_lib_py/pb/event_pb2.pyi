from typing import ClassVar as _ClassVar
from typing import Optional as _Optional

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message

DESCRIPTOR: _descriptor.FileDescriptor

class Event(_message.Message):
    __slots__ = ("name", "payload", "retry_count", "valid_till")
    NAME_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    RETRY_COUNT_FIELD_NUMBER: _ClassVar[int]
    VALID_TILL_FIELD_NUMBER: _ClassVar[int]
    name: str
    payload: bytes
    retry_count: int
    valid_till: str
    def __init__(
        self,
        name: _Optional[str] = ...,
        payload: _Optional[bytes] = ...,
        retry_count: _Optional[int] = ...,
        valid_till: _Optional[str] = ...,
    ) -> None: ...
