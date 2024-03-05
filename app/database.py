from __future__ import annotations

import asyncio
import datetime
import logging
import os
from typing import Any

from app.rdb.parser import RDBParser
from app.redis_config import RedisConfig


class RedisDatabase:
    def __init__(
        self,
        config: RedisConfig,
        data: dict[str, tuple[str, datetime.datetime | None]],
    ) -> None:
        self.config = config
        self.data = data

        self.cleanup_task = asyncio.create_task(
            self.expire_keys_periodically(interval=60)
        )

        self.replicas: dict[tuple[str, str], asyncio.StreamWriter] = {}
        self.command_queue: list[bytes] = []

        self.flush_task = asyncio.create_task(self.flush_buffer_periodically())

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
    def init_slave(cls, config: RedisConfig, rdb_data: bytes | None) -> RedisDatabase:
        data = RDBParser.load_from_bytes(rdb_data) if rdb_data else {}
        return cls(config, data)

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

    def add_command_to_queue(self, raw_command: bytes):
        self.command_queue.append(raw_command)

    async def flush_buffer_periodically(self):
        while True:
            await asyncio.sleep(1)
            await self.flush_command_buffer()

    async def flush_command_buffer(self) -> None:
        if not self.command_queue:
            return

        for client_id, writer in self.replicas.items():
            try:
                for command in self.command_queue:
                    if command:
                        writer.write(command)
                        await writer.drain()

            except ConnectionError as e:
                logging.error(
                    f"Failed to send commands to replica {client_id}: Connection Error - {e}"
                )
            except Exception as e:
                logging.critical(
                    f"Unexpected critical error with replica {client_id}: {e}"
                )

        # Clear the buffer after successful replication
        self.command_queue.clear()

    async def close(self) -> None:
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass

        if self.flush_task:
            self.flush_task.cancel()
            try:
                await self.flush_task
            except asyncio.CancelledError:
                pass
