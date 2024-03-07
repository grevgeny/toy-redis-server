from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from functools import partial
from typing import Awaitable, Callable

from app.commands import Command, PsyncCommand, SetCommand, parse_command
from app.rdb import data_loading
from app.redis_config import RedisConfig
from app.replication_manager import ReplicationManager
from app.resp.decoder import RESPDecoder
from app.storage import Storage

DecodedCommand = list[str]
ParseCommandFn = Callable[[DecodedCommand], Awaitable[Command]]


class Server(ABC):
    def __init__(self, host: str, port: int, parse_command_fn: ParseCommandFn) -> None:
        self.host = host
        self.port = port
        self.parse_command_fn = parse_command_fn

    @classmethod
    @abstractmethod
    async def from_config(cls, config: RedisConfig) -> Server:
        pass

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )

        async with server:
            await server.serve_forever()

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peername = writer.get_extra_info("peername")

        try:
            while data := await reader.read(1024):
                if not data:
                    response = b"-ERR no command provided\r\n"
                else:
                    response = await self.handle_command(data, writer)

                writer.write(response)
                await writer.drain()

        except Exception as e:
            logging.error(f"Error with {peername}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Disconnected by {peername}")

    @abstractmethod
    async def handle_command(self, data: bytes, writer: asyncio.StreamWriter) -> bytes:
        pass


class MasterServer(Server):
    def __init__(self, host: str, port: int, parse_command_fn: ParseCommandFn) -> None:
        super().__init__(
            host,
            port,
            parse_command_fn,
        )
        self.replica_writers: list[asyncio.StreamWriter] = []
        self.command_queue: list[bytes] = []
        self.command_propagation_task = asyncio.create_task(self.propagate_commands())

    async def propagate_commands(self) -> None:
        while True:
            await asyncio.sleep(1)

            if not self.command_queue:
                continue

            for writer in self.replica_writers:
                try:
                    for command in self.command_queue:
                        writer.write(command)
                        await writer.drain()
                except Exception as e:
                    logging.error(f"Error propagating command to replica: {e}")

            self.command_queue.clear()

    @classmethod
    async def from_config(cls, config: RedisConfig) -> MasterServer:
        data = data_loading.load_init_data_for_master(config)
        storage = Storage(data)
        parse_command_fn = partial(parse_command, storage, config)

        return cls(config.host, config.port, parse_command_fn)

    async def handle_command(self, data: bytes, writer: asyncio.StreamWriter) -> bytes:
        decoded_command: list[str] = RESPDecoder.decode(data)[0]
        command = await self.parse_command_fn(decoded_command)

        if isinstance(command, PsyncCommand):
            self.replica_writers.append(writer)
        elif isinstance(command, SetCommand):
            self.command_queue.append(data)

        return await command.execute()


class ReplicaServer(Server):
    def __init__(
        self,
        host: str,
        port: int,
        parse_command_fn: ParseCommandFn,
        replication_manager: ReplicationManager,
    ) -> None:
        super().__init__(host, port, parse_command_fn)
        self.replication_manager = replication_manager

    @classmethod
    async def from_config(cls, config: RedisConfig) -> ReplicaServer:
        replication_manager = await ReplicationManager.initialize(config)

        if not replication_manager or not replication_manager.is_connected:
            raise ConnectionError("Replica could not connect to master.")

        data = data_loading.load_init_data_for_replica(replication_manager.initial_data)
        storage = Storage(data)

        parse_command_fn = partial(parse_command, storage, config)
        replication_manager = replication_manager.set_replica_storage(storage)

        return cls(config.host, config.port, parse_command_fn, replication_manager)

    async def start(self) -> None:
        asyncio.create_task(self.replication_manager.start_replication())
        await super().start()

    async def handle_command(self, data: bytes, writer: asyncio.StreamWriter) -> bytes:
        decoded_command: list[str] = RESPDecoder.decode(data)[0]
        command = await self.parse_command_fn(decoded_command)

        return await command.execute()
