from __future__ import annotations

import asyncio
import datetime
from typing import Any


class Storage:
    def __init__(
        self,
        data: dict[str, tuple[str, datetime.datetime | None]],
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
        self.data[key] = (value, expiry)

    async def get(self, key: str) -> str | None:
        value, expiry = self.data.get(key, (None, None))
        if expiry and expiry < datetime.datetime.now(datetime.UTC):
            await self.delete(key)
            return None
        return value

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
                key for key, (_, expiry) in self.data.items() if expiry and expiry < now
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
