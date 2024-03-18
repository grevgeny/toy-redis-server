from __future__ import annotations

import asyncio
import datetime
from typing import Any

from toy_redis_server.data_types import Data, Stream, StreamEntry, String


class Storage:
    def __init__(
        self,
        data: Data,
    ) -> None:
        self.data = data
        self.cleanup_task = asyncio.create_task(self.expire_keys(interval=60))

    async def set(self, key: str, value: Any, expiry_ms: int | None = None) -> None:
        expiry = (
            (
                datetime.datetime.now(datetime.UTC)
                + datetime.timedelta(milliseconds=expiry_ms)
            )
            if expiry_ms
            else None
        )
        self.data[key] = String(key, value, expiry)

    async def xadd(
        self, stream_key: str, stream_entry_id: str, stream_entry: dict[str, str]
    ) -> None:
        stream = self.data.setdefault(stream_key, Stream(stream_key, []))
        entries = stream.entries if isinstance(stream, Stream) else []
        entries.append(StreamEntry(stream_entry_id, stream_entry))
        self.data[stream_key] = stream

    async def get(self, key: str) -> String | Stream | None:
        entry = self.data.get(key, None)
        if entry is None:
            return None

        if isinstance(entry, String):
            if entry.expiry and entry.expiry < datetime.datetime.now(datetime.UTC):
                await self.delete(key)
                return None

        return entry

    async def delete(self, key: str) -> int:
        if key in self.data:
            del self.data[key]
            return 1
        return 0

    async def keys(self) -> list[str]:
        return list(self.data.keys())

    async def expire_keys(self, interval: int) -> None:
        while True:
            await asyncio.sleep(interval)

            now = datetime.datetime.now(datetime.UTC)
            keys_to_expire = [
                key
                for key, entry in self.data.items()
                if entry.expiry and entry.expiry < now
            ]
            for key in keys_to_expire:
                await self.delete(key)

    async def close(self) -> None:
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
