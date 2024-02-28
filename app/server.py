from __future__ import annotations

import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase
from app.redis_config import RedisConfig
from app.resp.encoder import RESPEncoder


class MasterRedisServer:
    def __init__(self, redis_config: RedisConfig) -> None:
        self.redis_config = redis_config
        self.redis_database = RedisDatabase.init_master(self.redis_config)
        self.command_handler = CommandHandler(self.redis_database)

    async def start(self) -> None:
        host, port = self.redis_config.host, self.redis_config.port

        server = await asyncio.start_server(self.handle_connection, host, port)
        logging.info(f"Server started on {host}:{port}")

        async with server:
            await server.serve_forever()

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"New connection by {addr}")

        try:
            while command := await reader.read(1024):
                if self.command_handler:
                    response = await self.command_handler.handle_command(
                        command, writer
                    )
                    writer.write(response)
                    await writer.drain()
                else:
                    logging.error("Command handler not initialized")
                    break
        except ConnectionError:
            logging.error(f"Connection error with {addr}")
        except Exception as e:
            logging.error(f"Unexpected error with {addr}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Disconnected by {addr}")


class SlaveRedisServer:
    def __init__(self, redis_config: RedisConfig) -> None:
        self.redis_config = redis_config
        self.redis_database: RedisDatabase | None = None
        self.command_handler: CommandHandler | None = None
        self.replication_task: asyncio.Task | None = None

    async def start(self) -> None:
        slave_reader, slave_writer = await self.connect_to_master()

        success, rdb_data = await self.perform_handshake(slave_reader, slave_writer)
        if not success or rdb_data is None:
            return

        self.redis_database = RedisDatabase.init_slave(self.redis_config, rdb_data)
        self.command_handler = CommandHandler(self.redis_database)

        self.replication_task = asyncio.create_task(
            self.listen_to_master(slave_reader, slave_writer)
        )

        host, port = self.redis_config.host, self.redis_config.port

        server = await asyncio.start_server(self.handle_connection, host, port)
        logging.info(f"Server started on {host}:{port}")

        async with server:
            await server.serve_forever()

    async def connect_to_master(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(
            self.redis_config.master_host, self.redis_config.master_port
        )
        logging.info("Connected to master for initial sync and command replication")
        return reader, writer

    async def perform_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> tuple[bool, bytes | None]:
        # Ping command
        writer.write(RESPEncoder.encode_array("PING"))
        await writer.drain()

        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+PONG":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

        # Configure listening port
        writer.write(
            RESPEncoder.encode_array(
                "REPLCONF", "listening-port", str(self.redis_config.port)
            )
        )
        await writer.drain()

        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

        # Capability negotiation
        writer.write(RESPEncoder.encode_array("REPLCONF", "capa", "npsync2"))
        await writer.drain()

        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

        # Attempt PSYNC
        run_id = "?"
        offset = "-1"

        writer.write(RESPEncoder.encode_array("PSYNC", run_id, offset))
        await writer.drain()

        # Read the response to the PSYNC command
        response_line = await reader.readline()
        response = response_line.decode().strip()

        if response.startswith("+FULLRESYNC"):
            length_line = await reader.readline()
            length_str = length_line.decode().strip()
            _, rdb_length = length_str.split("$")
            rdb_length = int(rdb_length)

            rdb_data = await reader.readexactly(rdb_length)

            return True, rdb_data
        else:
            logging.error("PSYNC did not result in a FULLRESYNC response.")
            return False, None

    async def listen_to_master(
        self, slave_reader: asyncio.StreamReader, slave_writer: asyncio.StreamWriter
    ) -> None:
        if not self.command_handler:
            return

        try:
            while command := await slave_reader.read(1024):
                await self.command_handler.handle_command(command)
        except Exception as e:
            logging.error(f"Error while listening to master: {e}")

            if slave_writer:
                try:
                    slave_writer.close()
                    await slave_writer.wait_closed()
                    logging.info("Closed connection with master.")
                except Exception as e:
                    logging.error(f"Error closing connection with master: {e}")

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logging.info(f"Connected by {addr}")

        try:
            while command := await reader.read(1024):
                if self.command_handler:
                    response = await self.command_handler.handle_command(command)
                    writer.write(response)
                    await writer.drain()
                else:
                    logging.error("Command handler not initialized")
                    break
        except ConnectionError:
            logging.error(f"Connection error with {addr}")
        except Exception as e:
            logging.error(f"Unexpected error with {addr}: {str(e)}")
        finally:
            writer.close()
            await writer.wait_closed()
            logging.info(f"Disconnected by {addr}")
