import os

from toy_redis_server.data_types import Data
from toy_redis_server.rdb.parser import RDBParser


def load_init_data_for_master(rdb_dir: str | None, rdb_filename: str | None) -> Data:
    if rdb_dir and rdb_filename:
        file_path = os.path.join(rdb_dir, rdb_filename)
        if os.path.exists(file_path):
            return RDBParser.load_from_file(file_path)
    return {}


def load_init_data_for_replica(rdb_data: bytes | None) -> Data:
    return RDBParser.load_from_bytes(rdb_data) if rdb_data else {}
