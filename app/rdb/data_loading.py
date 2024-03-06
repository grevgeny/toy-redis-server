import os

from app.rdb.parser import RDBParser
from app.redis_config import RedisConfig


def load_init_data_for_master(config: RedisConfig) -> dict:
    if config.rdb_dir and config.rdb_filename:
        file_path = os.path.join(config.rdb_dir, config.rdb_filename)
        if os.path.exists(file_path):
            return RDBParser.load_from_file(file_path)
    return {}


def load_init_data_for_replica(rdb_data: bytes | None) -> dict:
    return RDBParser.load_from_bytes(rdb_data) if rdb_data else {}
