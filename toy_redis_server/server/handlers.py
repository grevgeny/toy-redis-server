from toy_redis_server.data_types import String
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
    if (entry := await storage.get(key)) and isinstance(entry, String):
        return RESPEncoder.encode_bulk_string(entry.value)

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
    entry = await storage.get(key)

    if not entry:
        return RESPEncoder.encode_bulk_string("none")

    return RESPEncoder.encode_bulk_string(str(type(entry)))


async def handle_xadd(
    storage: Storage, stream_key: str, stream_entry_id: str, values: list[str]
) -> None:
    stream_entry = dict(zip(values[::2], values[1::2]))
    await storage.xadd(stream_key, stream_entry_id, stream_entry)
