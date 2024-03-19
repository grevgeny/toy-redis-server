from typing import cast

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
    entry = await storage.get(key)
    if isinstance(entry, String):
        return RESPEncoder.encode_bulk_string(entry.value)

    return RESPEncoder.encode_null()


async def handle_del(storage: Storage, *keys: str) -> bytes:
    total_deleted = sum([await storage.delete(key) for key in keys])
    return RESPEncoder.encode_integer(total_deleted)


async def handle_keys(storage: Storage, arg: str) -> bytes:
    if arg != "*":
        return RESPEncoder.encode_error("Unknown subcommand")

    keys = await storage.keys()
    return RESPEncoder.encode_array(*keys)


async def handle_type(storage: Storage, key: str) -> bytes:
    entry_type = type(await storage.get(key))

    if entry_type.__name__ == "NoneType":
        return RESPEncoder.encode_bulk_string("none")

    return RESPEncoder.encode_bulk_string(str(entry_type))


async def handle_xadd(storage: Storage, stream_key: str, *args: str) -> bytes | None:
    stream_entry_id = args[0]

    try:
        stream_entry_id = process_stream_entry_id(stream_key, stream_entry_id, storage)
    except ValueError as e:
        return RESPEncoder.encode_error(str(e))

    values = args[1:]
    stream_entry = dict(zip(values[::2], values[1::2]))
    await storage.xadd(stream_key, stream_entry_id, stream_entry)

    return RESPEncoder.encode_bulk_string(stream_entry_id)


def process_stream_entry_id(
    stream_key: str, stream_entry_id: str, storage: Storage
) -> str:
    if stream_entry_id == "0-0":
        raise ValueError("The ID specified in XADD must be greater than 0-0")

    stream = cast(Stream | None, storage.data.get(stream_key))

    if stream and "*" not in stream_entry_id:
        last_entry_id = stream.entries[-1].key
        validate_stream_entry_id(stream_entry_id, last_entry_id)

    return calculate_next_stream_entry_id(stream_entry_id, stream)


def validate_stream_entry_id(proposed_id: str, last_id: str) -> None:
    proposed_ms, proposed_seq = map(int, proposed_id.split("-"))
    last_ms, last_seq = map(int, last_id.split("-"))

    if proposed_ms < last_ms or (proposed_ms == last_ms and proposed_seq <= last_seq):
        raise ValueError(
            "The ID specified in XADD is equal or smaller than the target stream top item"
        )


def calculate_next_stream_entry_id(proposed_id: str, stream: Stream | None) -> str:
    ms_time, seq_num = proposed_id.split("-")
    if seq_num == "*":
        seq_num = calculate_sequential_number(ms_time, stream)
    return f"{ms_time}-{seq_num}"


def calculate_sequential_number(ms_time: str, stream: Stream | None) -> int:
    if not stream:
        return 0 if ms_time != "0" else 1
    last_entry_id = stream.entries[-1].key
    last_ms_time, last_seq_num = map(int, last_entry_id.split("-"))
    return last_seq_num + 1 if ms_time == str(last_ms_time) else 0
