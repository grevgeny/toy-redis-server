import datetime
import os
from typing import Any

from app.rdb.parser import RDBParser


class RedisDatabase:
    def __init__(self, rdb_file_path: str | None = None) -> None:
        self.data: dict[str, tuple[Any, datetime.datetime | None]] = {}

        if rdb_file_path and os.path.exists(rdb_file_path):
            self.load_rdb_file(rdb_file_path)

    def load_rdb_file(self, filepath: str) -> None:
        """
        Load an RDB file into the database.
        """
        self.data = RDBParser.load_from_file(filepath)

    async def set(self, key: str, value: Any, expiry_ms: int | None = None) -> None:
        """
        Set the value of a key with an optional expiry time in milliseconds.
        """
        expiry = (
            datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(milliseconds=expiry_ms)
            if expiry_ms
            else None
        )
        self.data[key] = (value, expiry)

    async def get(self, key: str) -> Any | None:
        """
        Get the value of a key if it hasn't expired.
        """
        value, expiry = self.data.get(key, (None, None))
        if expiry and expiry < datetime.datetime.now(datetime.timezone.utc):
            await self.delete(key)  # Expire the key
            return None
        return value

    async def delete(self, key: str) -> None:
        """
        Delete a key from the database.
        """
        if key in self.data:
            del self.data[key]

    async def keys(self) -> list[str]:
        """
        Get all keys in the database.
        """
        return list(self.data.keys())
