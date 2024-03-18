import logging

from toy_redis_server.data_types import Stream, String
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
) -> bytes | None:
    error = validate_stream_entry_id(stream_key, stream_entry_id, storage)
    logging.info(f"XADD error: {error}")
    if error:
        return error

    stream_entry = dict(zip(values[::2], values[1::2]))
    await storage.xadd(stream_key, stream_entry_id, stream_entry)

    return RESPEncoder.encode_bulk_string(stream_entry_id)


def validate_stream_entry_id(
    stream_key: str, stream_entry_id: str, storage: Storage
) -> bytes | None:
    if stream_entry_id == "0-0":
        return b"-ERR The ID specified in XADD must be greater than 0-0\r\n"

    stream = storage.data.get(stream_key, None)
    if stream and isinstance(stream, Stream):
        stream_last_entry = stream.entries[-1]
    else:
        stream_last_entry = None

    if not stream_last_entry:
        return None

    ms_time_last_entry, seq_num_last_entry = stream_last_entry.key.split("-")
    ms_time, seq_num = stream_entry_id.split("-")

    if int(ms_time) < int(ms_time_last_entry):
        return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"

    if int(ms_time) == int(ms_time_last_entry) and int(seq_num) <= int(
        seq_num_last_entry
    ):
        return b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
