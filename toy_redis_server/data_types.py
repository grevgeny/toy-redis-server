from __future__ import annotations

import datetime
from dataclasses import dataclass


class RedisType(type):
    def __repr__(self) -> str:
        return self.__name__.lower()


@dataclass
class String(metaclass=RedisType):
    key: str
    value: str
    expiry: datetime.datetime | None = None

    def __len__(self) -> int:
        return len(self.value)


@dataclass
class Stream(metaclass=RedisType):
    key: str
    entries: list[StreamEntry]
    expiry: datetime.datetime | None = None


@dataclass
class StreamEntry:
    key: str
    entry: dict[str, str]


Data = dict[str, String | Stream]
