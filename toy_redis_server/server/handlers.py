import logging

from toy_redis_server.resp.encoder import RESPEncoder
from toy_redis_server.storage import Storage


def handle_ping() -> bytes:
    return RESPEncoder.encode_simple_string("PONG")


def handle_echo(*args: str) -> bytes:
    return RESPEncoder.encode_bulk_string(" ".join(args))


async def handle_set(
    storage: Storage, key: str, value: str, expiry_ms: int | None = None
) -> bytes:
    await storage.set(key, value, expiry_ms)
    return RESPEncoder.encode_simple_string("OK")


async def handle_get(storage: Storage, key: str) -> bytes:
    if value := await storage.get(key):
        return RESPEncoder.encode_bulk_string(value)

    return RESPEncoder.encode_null()


async def handle_del(storage: Storage, *keys: str) -> bytes:
    total_deleted = sum([await storage.delete(key) for key in keys])
    return RESPEncoder.encode_integer(total_deleted)


async def handle_keys(storage: Storage, arg: str) -> bytes:
    if arg != "*":
        return b"-ERR unknown subcommand\r\n"

    keys = await storage.keys()
    return RESPEncoder.encode_array(*keys)


async def handle_type(storage: Storage, key: str) -> bytes:
    TYPE_MAPPING = {
        "str": "string",
        None: "none",
    }

    value = await storage.get(key)
    redis_type = TYPE_MAPPING.get(value, "unknown")

    if redis_type == "unknown":
        logging.info(f"Unknown type for key {key}: {value}")

    return RESPEncoder.encode_simple_string(redis_type)
