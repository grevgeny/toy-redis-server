from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from app.commands import Command, ReplconfCommand
from app.resp.decoder import RESPDecoder
from app.resp.encoder import RESPEncoder

DecodedCommand = list[str]
ParseCommandFn = Callable[[DecodedCommand], Awaitable[Command]]


class ReplicationManager:
    def __init__(self, master_host: str, master_port: int, slave_port: int) -> None:
        self.master_host = master_host
        self.master_port = master_port
        self.slave_port = slave_port

        self.offset = -1

        self.reader: asyncio.StreamReader
        self.writer: asyncio.StreamWriter

    async def connect_to_master(self) -> None:
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.master_host, self.master_port
            )
            await self.perform_handshake()
        except Exception as e:
            logging.error(f"Failed to connect to master: {e}")

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

        self.writer.write(RESPEncoder.encode_array("PSYNC", run_id, str(self.offset)))
        await self.writer.drain()
        self.offset += 1

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

    def start_replication_task(self, parse_command_fn: ParseCommandFn) -> None:
        self.replication_task = asyncio.create_task(
            self.start_replication(parse_command_fn)
        )

    async def start_replication(self, parse_command_fn: ParseCommandFn) -> None:
        try:
            while data := await self.reader.read(1024):
                if not data:
                    continue

                decoded_commands = RESPDecoder.decode(data)
                for decoded_command in decoded_commands:
                    command = await parse_command_fn(decoded_command)

                    if isinstance(command, ReplconfCommand):
                        self.writer.write(
                            RESPEncoder.encode_array(
                                "REPLCONF", "ACK", str(self.offset)
                            )
                        )
                        await self.writer.drain()

                    await command.execute()

                    self.offset += len(RESPEncoder.encode_array(*decoded_command))

        except Exception as e:
            logging.error(f"Replication error: {e}")
