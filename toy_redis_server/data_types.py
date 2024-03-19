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

    def __getitem__(self, key: str | slice) -> list[list[str | list[str]]]:
        if isinstance(key, slice):
            start = key.start
            end = key.stop

            return [entry.dump() for entry in self.entries if start <= entry.key <= end]

        else:
            return [entry.dump() for entry in self.entries if entry.key == key]


@dataclass
class StreamEntry:
    key: str
    entry: dict[str, str]

    def dump(self) -> list[str | list[str]]:
        return [self.key, [item for pair in self.entry.items() for item in pair]]


Data = dict[str, String | Stream]
