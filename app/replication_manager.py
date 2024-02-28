import asyncio
import logging

from app.redis_config import RedisConfig
from app.resp.encoder import RESPEncoder


class ReplicationManager:
    def __init__(self, config: RedisConfig) -> None:
        self.config = config

        self.writer: asyncio.StreamWriter
        self.reader: asyncio.StreamReader

    async def connect_to_master(self) -> tuple[bool, bytes | None]:
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.config.master_host, self.config.master_port
            )
            logging.info("Connected to master for initial sync and command replication")

            handshake_successful, rdb_data = await self.perform_handshake()
            if handshake_successful:
                logging.info("Successful handshake with master.")
                return True, rdb_data
            else:
                logging.error("Handshake with master failed.")
                return False, None

        except asyncio.TimeoutError:
            logging.error("Timeout error when connecting to master.")
            return False, None
        except (ConnectionRefusedError, ConnectionResetError):
            logging.error("Connection to master refused or reset.")
            return False, None
        except Exception as e:
            logging.error(f"Error connecting to master: {e}")
            return False, None

    async def perform_handshake(self) -> tuple[bool, bytes | None]:
        # Ping command
        self.writer.write(RESPEncoder.encode_array("PING"))
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+PONG":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

        # Configure listening port
        self.writer.write(
            RESPEncoder.encode_array(
                "REPLCONF", "listening-port", str(self.config.port)
            )
        )
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

        # Capability negotiation
        self.writer.write(RESPEncoder.encode_array("REPLCONF", "capa", "npsync2"))
        await self.writer.drain()

        response = await self.reader.read(1024)
        decoded_response = response.decode().strip()
        if decoded_response != "+OK":
            logging.warning(f"Unexpected response from master: {response}")
            return False, None

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
            rdb_length = int(rdb_length)

            rdb_data = await self.reader.readexactly(rdb_length)

            return True, rdb_data
        else:
            logging.error("PSYNC did not result in a FULLRESYNC response.")
            return False, None

    async def close_connection(self) -> None:
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
                logging.info("Closed connection with master.")
            except Exception as e:
                logging.error(f"Error closing connection with master: {e}")
