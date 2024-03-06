from __future__ import annotations

import asyncio
import logging

from app.command_handler import CommandHandler
from app.rdb import data_loading
from app.redis_config import RedisConfig
from app.replication_manager import ReplicationManager
from app.storage import Storage


class Server:
    def __init__(self, host: str, port: int, command_handler: CommandHandler) -> None:
        self.host = host
        self.port = port
        self.command_handler = command_handler

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
            while command := await reader.read(1024):
                response = await self.command_handler.handle_command(command, writer)
                writer.write(response)
                await writer.drain()
        except Exception as e:
            logging.error(f"Error with {peername}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Disconnected by {peername}")


class MasterRedisServer(Server):
    def __init__(
        self,
        host: str,
        port: int,
        command_handler: CommandHandler,
    ) -> None:
        super().__init__(
            host,
            port,
            command_handler,
        )

    @classmethod
    def from_config(cls, redis_config: RedisConfig) -> MasterRedisServer:
        data = data_loading.load_init_data_for_master(redis_config)
        storage = Storage(data)
        command_handler = CommandHandler(storage, redis_config)

        return cls(
            redis_config.host,
            redis_config.port,
            command_handler,
        )


class SlaveRedisServer(Server):
    def __init__(
        self,
        host: str,
        port: int,
        command_handler: CommandHandler,
        replication_manager: ReplicationManager,
    ) -> None:
        super().__init__(host, port, command_handler)
        self.replication_manager = replication_manager

    @classmethod
    async def from_config(cls, redis_config: RedisConfig) -> SlaveRedisServer:
        replication_manager = await ReplicationManager.initialize(redis_config)

        if not replication_manager or not replication_manager.is_connected:
            raise ConnectionError("Replica could not connect to master.")

        data = data_loading.load_init_data_for_replica(replication_manager.initial_data)
        storage = Storage(data)

        command_handler = CommandHandler(storage, redis_config)
        replication_manager = replication_manager.set_replica_storage(storage)

        return cls(
            redis_config.host, redis_config.port, command_handler, replication_manager
        )

    async def start(self) -> None:
        asyncio.create_task(self.replication_manager.start_replication())
        await super().start()
