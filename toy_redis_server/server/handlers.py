import time
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


async def handle_xrange(
    storage: Storage, stream_key: str, start: str, end: str
) -> bytes:
    stream = cast(Stream | None, await storage.get(stream_key))

    if not stream:
        return RESPEncoder.encode_null()

    if "-" not in start:
        start = f"{start}-0"
    elif start == "-":
        start = "0-0"

    if end == "+":
        end = f"{round(time.time() * 1000)}-{len(stream.entries) - 1}"
    elif "-" not in end:
        end = f"{end}-{len(stream.entries) - 1}"

    found_entries = stream[start:end]

    return RESPEncoder.encode_array(*found_entries)


async def handle_xread(storage: Storage, *args: str) -> bytes:
    n_streams = len(args) // 2
    streams = zip(args[:n_streams], args[n_streams:])

    stream_responses = []
    for stream_key, start in streams:
        stream = cast(Stream | None, await storage.get(stream_key))

        if not stream:
            continue

        start = f"{start.split('-')[0]}-{int(start.split('-')[1]) + 1}"
        end = f"{round(time.time() * 1000)}-{len(stream.entries) - 1}"

        found_entries = stream[start:end]

        if not found_entries:
            continue

        stream_responses.append([stream_key, found_entries])

    return RESPEncoder.encode_array(*stream_responses)


def process_stream_entry_id(
    stream_key: str, stream_entry_key: str, storage: Storage
) -> str:
    if stream_entry_key == "0-0":
        raise ValueError("The ID specified in XADD must be greater than 0-0")

    last_ms_time, last_seq_num = get_last_stream_entry_key(stream_key, storage)

    if "*" in stream_entry_key:
        return calculate_next_stream_entry_id(
            stream_entry_key, last_ms_time, last_seq_num
        )
    else:
        validate_stream_entry_key(stream_entry_key, last_ms_time, last_seq_num)
        return stream_entry_key


def get_last_stream_entry_key(stream_key: str, storage: Storage) -> tuple[int, int]:
    stream = cast(Stream | None, storage.data.get(stream_key))

    if stream:
        last_entry = stream.entries[-1]
        ms_time, seq_num = last_entry.key.split("-")
        return int(ms_time), int(seq_num)
    else:
        return 0, 0


def validate_stream_entry_key(
    proposed_id: str, last_ms_time: int, last_seq_num: int
) -> None:
    proposed_ms, proposed_seq = map(int, proposed_id.split("-"))
    if proposed_ms < last_ms_time or (
        proposed_ms == last_ms_time and proposed_seq <= last_seq_num
    ):
        raise ValueError(
            "The ID specified in XADD is equal or smaller than the target stream top item"
        )


def calculate_next_stream_entry_id(
    stream_entry_key: str, last_ms_time: int, last_seq_num: int
) -> str:
    if stream_entry_key == "*":
        ms_time = round(time.time() * 1000)
        seq_num = last_seq_num + 1 if ms_time == last_ms_time else 0
    else:
        ms_time, _ = stream_entry_key.split("-")
        seq_num = last_seq_num + 1 if last_ms_time == int(ms_time) else 0

    return f"{ms_time}-{seq_num}"


"""

*1\r\n
    *2\r\n
        $8\r\nsome_key\r\n
        
        *1\r\n
            *2\r\n
                $3\r\0-1\r\n
                *2\r\n
                    $11\r\ntemperature\r\n
                    $2\r\n37\r\n

                
        *1\r\n
            *2\r\n
                $3\r\n0-1\r\n
                *2\r\n
                    $11\r\ntemperature\r\n
                    $2\r\n46\r\n

    *2\r\n
        $6\r\norange\r\n
        *1\r\n
            *2\r\n
                $3\r\n0-1\r\n
                *2\r\n
                    $11\r\ntemperature\r\n
                    $2\r\n52\r\n

"""
