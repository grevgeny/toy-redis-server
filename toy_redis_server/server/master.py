from __future__ import annotations

import asyncio
import base64
import secrets
from typing import NoReturn

from toy_redis_server.rdb import data_loading
from toy_redis_server.resp.decoder import RESPDecoder
from toy_redis_server.resp.encoder import RESPEncoder
from toy_redis_server.server import handlers
from toy_redis_server.server.server import Role
from toy_redis_server.storage import Storage


def get_empty_rdb() -> bytes:
    EMPTY_RDS_BASE64 = """
        UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==
    """
    return base64.b64decode(EMPTY_RDS_BASE64)


class MasterServer:
    role: Role = Role.MASTER

    def __init__(
        self,
        host: str,
        port: int,
        dir: str | None,
        filename: str | None,
    ) -> None:
        self.host = host
        self.port = port
        self.dir = dir
        self.dbfilename = filename

    async def start(self) -> None:
        data = data_loading.load_init_data_for_master(self.dir, self.dbfilename)
        self.storage = Storage(data)

        self.master_repl_id: str = secrets.token_hex(40)
        self.master_repl_offset: int = 0

        self.replica_writers: dict[asyncio.StreamWriter, int] = {}
        self.command_queue: list[bytes] = []

        self.command_propagation_condition = asyncio.Condition()
        self.command_propagation_task = asyncio.create_task(self.propagate_commands())
        self.replica_ack_task = asyncio.create_task(
            self.request_replica_acks_regularly()
        )
        self.replica_acked_event = asyncio.Event()
        self.latest_up_to_date_replicas = 0

        self.server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        async with self.server:
            await self.server.serve_forever()

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while data := await reader.read(1024):
                if not data:
                    continue

                response = await self.handle_command(data, writer)

                if response:
                    writer.write(response)
                    await writer.drain()

        finally:
            writer.close()
            await writer.wait_closed()

    async def propagate_commands(self) -> NoReturn:
        async with self.command_propagation_condition:
            while True:
                await self.command_propagation_condition.wait()

                commands_to_propagate = self.command_queue.copy()
                self.command_queue.clear()

                for writer in self.replica_writers:
                    for command in commands_to_propagate:
                        writer.write(command)
                        await writer.drain()

                self.command_queue.clear()

    async def broadcast_command_to_replicas(self, command: bytes) -> None:
        async with self.command_propagation_condition:
            self.command_queue.append(command)
            self.master_repl_offset += len(command)
            self.command_propagation_condition.notify()

    async def wait_for_replicas(
        self, num_replicas: int, timeout_seconds: float
    ) -> None:
        start_time = asyncio.get_event_loop().time()

        while True:
            current_time = asyncio.get_event_loop().time()
            if current_time - start_time >= timeout_seconds:
                break

            self.latest_up_to_date_replicas = sum(
                1
                for offset in self.replica_writers.values()
                if offset >= self.master_repl_offset
            )

            if self.latest_up_to_date_replicas >= num_replicas:
                break

            await asyncio.sleep(0.01)

    async def request_replica_acks_regularly(self) -> NoReturn:
        while True:
            for writer in self.replica_writers:
                command = RESPEncoder.encode_array("REPLCONF", "GETACK", "*")
                writer.write(command)
                await writer.drain()

            await asyncio.sleep(0.1)

    async def handle_command(
        self, data: bytes, writer: asyncio.StreamWriter
    ) -> bytes | None:
        decoded_command: list[str] = RESPDecoder.decode(data)[0]
        if not decoded_command:
            response = b"-ERR no command provided\r\n"

        normalized_command = list(map(str.lower, decoded_command))

        match normalized_command:
            case ["ping"]:
                response = handlers.handle_ping()

            case ["echo", *args]:
                response = handlers.handle_echo(*args)

            case ["set", key, value]:
                response = await handlers.handle_set(self.storage, key, value)
                await self.broadcast_command_to_replicas(data)

            case ["set", key, value, "px", expiry_ms]:
                response = await handlers.handle_set(
                    self.storage, key, value, int(expiry_ms)
                )
                await self.broadcast_command_to_replicas(data)

            case ["get", key]:
                response = await handlers.handle_get(self.storage, key)

            case ["del", *keys]:
                response = await handlers.handle_del(self.storage, *keys)

            case ["keys", arg]:
                response = await handlers.handle_keys(self.storage, arg)

            case ["config", "get", "dir"]:
                if not self.dir:
                    response = RESPEncoder.encode_null()
                else:
                    response = RESPEncoder.encode_array("dir", self.dir)

            case ["config", "get", "dbfilename"]:
                if not self.dbfilename:
                    response = RESPEncoder.encode_null()
                else:
                    response = RESPEncoder.encode_array("dbfilename", self.dbfilename)

            case ["info", "replication"]:
                info_string = "\n".join(
                    [
                        f"role:{self.role.value}",
                        f"master_replid:{self.master_repl_id}",
                        f"master_repl_offset:{self.master_repl_offset}",
                    ]
                )
                response = RESPEncoder.encode_bulk_string(info_string)

            case ["replconf", "listening-port", _]:
                response = RESPEncoder.encode_simple_string("OK")

            case ["replconf", "capa", "psync2"]:
                response = RESPEncoder.encode_simple_string("OK")

            case ["replconf", "ack", offset]:
                self.replica_writers[writer] = int(offset)
                response = None

            case ["psync", "?", "-1"]:
                self.replica_writers[writer] = 0

                full_resync = RESPEncoder.encode_simple_string(
                    f"FULLRESYNC {self.master_repl_id} {self.master_repl_offset}"
                )
                empty_rdb = get_empty_rdb()

                response = full_resync + f"${len(empty_rdb)}\r\n".encode() + empty_rdb

            case ["wait", numreplicas, timeout_ms]:
                if self.master_repl_offset == 0:
                    response = RESPEncoder.encode_integer(len(self.replica_writers))

                timeout_seconds = int(timeout_ms) / 1000

                try:
                    await asyncio.wait_for(
                        self.wait_for_replicas(int(numreplicas), timeout_seconds),
                        timeout=timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

                response = RESPEncoder.encode_integer(self.latest_up_to_date_replicas)

            case ["type", key]:
                response = await handlers.handle_type(self.storage, key)

            case ["xadd", stream_key, *args]:
                entry_id = args[0] if len(args) % 2 != 0 else "random_id"
                values = args[1:]

                await handlers.handle_xadd(self.storage, stream_key, entry_id, values)

                return RESPEncoder.encode_bulk_string(entry_id) if entry_id else None

            case _:
                response = b"-ERR unknown command\r\n"

        return response

    async def stop(self) -> None:
        self.server.close()
        await self.server.wait_closed()

        self.command_propagation_task.cancel()

        try:
            await self.command_propagation_task
        except asyncio.CancelledError:
            pass

        self.replica_ack_task.cancel()

        try:
            await self.replica_ack_task
        except asyncio.CancelledError:
            pass

        for writer in self.replica_writers.keys():
            writer.close()
            await writer.wait_closed()

        self.replica_writers.clear()
