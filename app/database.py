import asyncio
import datetime
import os
from typing import Any

from app.rdb.parser import RDBParser
from app.redis_config import RedisConfig


class RedisDatabase:
    def __init__(self, config: RedisConfig) -> None:
        self.config = config
        self.data: dict[str, tuple[str, datetime.datetime | None]] = {}
        self._init_database()

    def _init_database(self) -> None:
        rdb_file_path = self._get_rdb_file_path()
        if rdb_file_path and os.path.exists(rdb_file_path):
            self.load_rdb_file(rdb_file_path)
        self.cleanup_task = asyncio.create_task(
            self.expire_keys_periodically(interval=60)
        )

    def _get_rdb_file_path(self) -> str | None:
        if self.config.rdb_dir and self.config.rdb_filename:
            return os.path.join(self.config.rdb_dir, self.config.rdb_filename)
        return None

    def load_rdb_file(self, filepath: str) -> None:
        self.data = RDBParser.load_from_file(filepath)

    async def set(self, key: str, value: Any, expiry_ms: int | None = None) -> None:
        expiry = (
            datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(milliseconds=expiry_ms)
            if expiry_ms
            else None
        )
        self.data[key] = (value, expiry)

    async def get(self, key: str) -> Any | None:
        value, expiry = self.data.get(key, (None, None))
        if expiry and expiry < datetime.datetime.now(datetime.UTC):
            await self.delete(key)
            return None
        return value

    async def delete(self, key: str) -> None:
        if key in self.data:
            del self.data[key]

    async def keys(self) -> list[str]:
        return list(self.data.keys())

    async def expire_keys_periodically(self, interval: int) -> None:
        while True:
            await asyncio.sleep(interval)
            now = datetime.datetime.now(datetime.timezone.utc)
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
