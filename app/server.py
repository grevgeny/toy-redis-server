from __future__ import annotations

import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase
from app.redis_config import RedisConfig
from app.replication_manager import ReplicationManager


class Server:
    def __init__(self, host: str, port: int, command_handler: CommandHandler) -> None:
        self.host = host
        self.port = port
        self.command_handler = command_handler

    async def start(self) -> None:
        server = await asyncio.start_server(
            self.handle_connection, self.host, self.port
        )
        logging.info(f"Server started on {self.host}:{self.port}")

        async with server:
            await server.serve_forever()

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"New connection by {addr}")

        try:
            while command := await reader.read(1024):
                response = await self.command_handler.handle_command(command, writer)
                writer.write(response)
                await writer.drain()
        except Exception as e:
            logging.error(f"Error with {addr}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Disconnected by {addr}")


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
        redis_database = RedisDatabase.init_master(redis_config)
        command_handler = CommandHandler(redis_database)
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
        super().__init__(
            host,
            port,
            command_handler,
        )
        self.replication_manager = replication_manager

    @classmethod
    async def from_config(cls, redis_config: RedisConfig) -> SlaveRedisServer:
        replication_manager = await ReplicationManager.initialize(redis_config)

        if replication_manager and replication_manager.is_connected:
            redis_database = RedisDatabase.init_slave(
                redis_config, replication_manager.initial_data
            )
            return cls(
                redis_config.host,
                redis_config.port,
                CommandHandler(redis_database),
                replication_manager,
            )
        else:
            logging.error("Failed to initialize replication manager.")
            raise ConnectionError("Cannot connect to master server.")

    async def start(self) -> None:
        await self.replication_manager.start_replication(self.command_handler)
        await super().start()
