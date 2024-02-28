from __future__ import annotations

import asyncio
import datetime
import os
from typing import Any

from app.exceptions import ReplicationInitializationError
from app.rdb.parser import RDBParser
from app.redis_config import RedisConfig
from app.replication_manager import ReplicationManager


class RedisDatabase:
    def __init__(
        self,
        config: RedisConfig,
        data: dict[str, tuple[str, datetime.datetime | None]],
    ) -> None:
        self.config = config
        self.data = data
        self.cleanup_task: asyncio.Task = asyncio.create_task(
            self.expire_keys_periodically(interval=60)
        )

    @classmethod
    def init_master(cls, config: RedisConfig) -> RedisDatabase:
        if config.rdb_dir and config.rdb_filename:
            file_path = os.path.join(config.rdb_dir, config.rdb_filename)
        else:
            file_path = None

        if file_path and os.path.exists(file_path):
            data = RDBParser.load_from_file(file_path)
        else:
            data = {}

        return cls(config, data)

    @classmethod
    async def init_slave(cls, config: RedisConfig) -> RedisDatabase:
        replication_manager = ReplicationManager(config)
        success, rdb_data = await replication_manager.connect_to_master()

        if success and rdb_data is not None:
            data = RDBParser.load_from_bytes(rdb_data)
            return cls(config, data)
        else:
            await replication_manager.close_connection()
            raise ReplicationInitializationError(
                "Failed to initialize replication from master."
            )

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

    async def delete(self, key: str) -> int:
        if key in self.data:
            del self.data[key]
            return 1
        return 0

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
