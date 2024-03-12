from __future__ import annotations

import asyncio
import logging

from app.rdb import data_loading
from app.resp.decoder import RESPDecoder
from app.resp.encoder import RESPEncoder
from app.server import handlers
from app.server.server import Role
from app.storage import Storage


class ReplicaServer:
    role: Role = Role.REPLICA

    def __init__(
        self, host: str, port: int, master_host: str, master_port: int
    ) -> None:
        self.host = host
        self.port = port
        self.master_host = master_host
        self.master_port = master_port

        self.repl_id: str = "?"
        self.offset: int = -1

    async def start(self) -> None:
        reader, writer = await self.connect_to_master()
        data = data_loading.load_init_data_for_replica(
            await self.perform_handshake(reader, writer)
        )
        self.storage = Storage(data)

        self.command_replication_task = asyncio.create_task(
            self.handle_connection(reader, writer, silent=True)
        )

        server = await asyncio.start_server(
            lambda r, w: self.handle_connection(r, w, silent=False),
            self.host,
            self.port,
        )

        async with server:
            await server.serve_forever()

    async def connect_to_master(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.open_connection(
            self.master_host, self.master_port
        )
        return reader, writer

    async def perform_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> bytes | None:
        # Ping command
        writer.write(RESPEncoder.encode_array("PING"))
        await writer.drain()
        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+PONG":
            logging.warning(f"Unexpected response from master: {response}")

        # Configure listening port
        writer.write(
            RESPEncoder.encode_array("REPLCONF", "listening-port", str(self.port))
        )
        await writer.drain()
        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")

        # Capability negotiation
        writer.write(RESPEncoder.encode_array("REPLCONF", "capa", "psync2"))
        await writer.drain()

        response = await reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")

        # Attempt PSYNC
        writer.write(RESPEncoder.encode_array("PSYNC", self.repl_id, str(self.offset)))
        await writer.drain()
        self.offset += 1

        # Read the response to the PSYNC command
        response_line = await reader.readline()
        response = response_line.decode().strip()

        if response.startswith("+FULLRESYNC"):
            length_line = await reader.readline()
            length_str = length_line.decode().strip()
            _, rdb_length = length_str.split("$")

            return await reader.readexactly(int(rdb_length))
        else:
            logging.error("PSYNC did not result in a FULLRESYNC response.")

    async def handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, silent: bool
    ) -> None:
        try:
            while data := await reader.read(1024):
                if not data:
                    continue

                for command in RESPDecoder.decode(data):
                    await self.handle_command(command, writer, silent)
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_command(
        self, command: list[str], writer: asyncio.StreamWriter, silent: bool
    ) -> None:
        if not command:
            return

        normalized_command = list(map(str.lower, command))

        match normalized_command:
            case ["set", key, value]:
                response = await handlers.handle_set(self.storage, key, value)

            case ["set", key, value, "px", expiry_ms]:
                response = await handlers.handle_set(
                    self.storage, key, value, int(expiry_ms)
                )

            case ["get", key]:
                response = await handlers.handle_get(self.storage, key)

            case ["info", "replication"]:
                info_string = "\n".join(
                    [
                        f"role:{self.role.value}",
                    ]
                )
                response = RESPEncoder.encode_bulk_string(info_string)

            case ["replconf", "getack", *_]:
                response = RESPEncoder.encode_array("REPLCONF", "ACK", str(self.offset))
                silent = False

            case _:
                response = b"-ERR unknown command\r\n"

        if not silent:
            writer.write(response)
            await writer.drain()

        self.offset += len(RESPEncoder.encode_array(*command))
