from __future__ import annotations

import asyncio
import logging

from app.redis_config import RedisConfig
from app.resp.decoder import RESPDecoder
from app.resp.encoder import RESPEncoder
from app.storage import Storage


class ReplicationManager:
    def __init__(self, master_host: str, master_port: int, slave_port: int) -> None:
        self.master_host = master_host
        self.master_port = master_port
        self.slave_port = slave_port

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        self.is_connected: bool = False
        self.initial_data: bytes | None = None
        self.replica_storage: Storage | None = None

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
        if not self.writer or not self.reader:
            return

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

    def set_replica_storage(self, storage: Storage) -> ReplicationManager:
        self.replica_storage = storage
        return self

    async def start_replication(self) -> None:
        if (
            not self.is_connected
            or not self.replica_storage
            or not self.reader
            or not self.writer
        ):
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

                    normalized_command = [
                        decoded_command[0].lower(),
                        *map(
                            lambda x: x.lower(),
                            decoded_command[1:],
                        ),
                    ]

                    match normalized_command:
                        case ["set", *args]:
                            key, value = args[0], args[1]
                            expiry_ms = int(args[3]) if len(args) > 3 else None
                            await self.replica_storage.set(key, value, expiry_ms)
                        case ["replconf", "getack", "*"]:
                            response = RESPEncoder.encode_array("REPLCONF", "ACK", "0")
                            self.writer.write(response)
                        case _:
                            logging.warning(
                                f"Unsupported command: {normalized_command}"
                            )
                            continue

        except Exception as e:
            logging.error(f"Replication error: {e}")
