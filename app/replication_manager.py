from __future__ import annotations

import asyncio
import logging

from app.database import RedisDatabase
from app.redis_config import RedisConfig
from app.resp.decoder import RESPDecoder
from app.resp.encoder import RESPEncoder


class ReplicationManager:
    def __init__(self, master_host: str, master_port: int, slave_port: int) -> None:
        self.master_host = master_host
        self.master_port = master_port
        self.slave_port = slave_port

        self.is_connected: bool = False
        self.initial_data: bytes | None = None
        self.slave_db: RedisDatabase | None = None

    @classmethod
    async def initialize(cls, redis_config: RedisConfig) -> ReplicationManager | None:
        if not (master_host := redis_config.master_host) or not (
            master_port := redis_config.master_port
        ):
            logging.info("Missing Host/Port for Master.")
            return None

        instance = cls(master_host, master_port, redis_config.port)
        await instance.connect_to_master()
        return instance

    async def connect_to_master(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.master_host, self.master_port
            )
            await self.perform_handshake()
            self.is_connected = True
        except Exception as e:
            logging.error(f"Failed to connect to master: {e}")
            self.is_connected = False

    async def perform_handshake(self) -> None:
        # Ping command
        self.writer.write(RESPEncoder.encode_array("PING"))
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+PONG":
            logging.warning(f"Unexpected response from master: {response}")

        # Configure listening port
        self.writer.write(
            RESPEncoder.encode_array("REPLCONF", "listening-port", str(self.slave_port))
        )
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")

        # Capability negotiation
        self.writer.write(RESPEncoder.encode_array("REPLCONF", "capa", "npsync2"))
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")

        # Attempt PSYNC
        run_id = "?"
        offset = "-1"

        self.writer.write(RESPEncoder.encode_array("PSYNC", run_id, offset))
        await self.writer.drain()

        # Read the response to the PSYNC command
        response_line = await self.reader.readline()
        response = response_line.decode().strip()

        if response.startswith("+FULLRESYNC"):
            length_line = await self.reader.readline()
            length_str = length_line.decode().strip()
            _, rdb_length = length_str.split("$")

            self.initial_data = await self.reader.readexactly(int(rdb_length))
        else:
            logging.error("PSYNC did not result in a FULLRESYNC response.")

    def set_slave_db(self, database: RedisDatabase) -> ReplicationManager:
        self.slave_db = database
        return self

    async def start_replication(self) -> None:
        if not self.is_connected or not self.slave_db:
            logging.error("ReplicationManager is not properly initialized.")
            return

        try:
            while data := await self.reader.read(1024):
                if not data:
                    break

                decoded_commands = RESPDecoder.decode(data)
                for decoded_command in decoded_commands:
                    if len(decoded_command) < 2:
                        continue

                    command, *args = decoded_command

                    if command.lower() == "set":
                        key, value = args[0], args[1]
                        expiry_ms = int(args[3]) if len(args) > 3 else None
                        await self.slave_db.set(key, value, expiry_ms)
                    else:
                        logging.warning(f"Unsupported command: {command}")
                        continue

        except Exception as e:
            logging.error(f"Replication error: {e}")
            self.connected = False