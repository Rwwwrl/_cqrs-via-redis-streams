from enum import Enum
from typing import Any, NewType

from pydantic import BaseModel

MessageResult = NewType('MessageResult', Any)


class DTO(BaseModel, frozen=True):
    pass


class MessageType(Enum):

    EVENT = 'EVENT'
    SYNC_COMMAND = 'SYNC_COMMAND'
    ASYNC_COMMAND = 'ASYNC_COMMAND'
    QUERY = 'QUERY'
