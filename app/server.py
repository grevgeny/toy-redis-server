import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase
from app.resp.decoder import RESPDecoder
from app.resp.encoder import RESPEncoder


async def start_server(host: str, port: int, db: RedisDatabase):
    async def on_client_connect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        await handle_connection(reader, writer, db)

    if (
        db.config.role == "slave"
        and (master_host := db.config.master_host)
        and (master_port := db.config.master_port)
    ):
        asyncio.create_task(connect_to_master(master_host, master_port, db.config.port))

    server = await asyncio.start_server(on_client_connect, host, port)
    logging.info(f"Server started on {host}:{port}")

    async with server:
        await server.serve_forever()


async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    db: RedisDatabase,
) -> None:
    command_handler = CommandHandler(db)
    addr = writer.get_extra_info("peername")
    logging.info(f"Connected by {addr}")

    try:
        while data := await reader.read(1024):
            response = await command_handler.handle_command(RESPDecoder.decode(data))
            writer.write(response)
            await writer.drain()

    except ConnectionError:
        logging.error(f"Connection error with {addr}")
    except Exception as e:
        logging.error(f"Unexpected error with {addr}: {str(e)}")
    finally:
        writer.close()
        await writer.wait_closed()
        logging.info(f"Disconnected by {addr}")


async def connect_to_master(master_host: str, master_port: int, port: int):
    writer = None
    try:
        reader, writer = await asyncio.open_connection(master_host, master_port)
        logging.info("Connected to master.")

        if await perform_handshake(reader, writer, port):
            logging.info("Successful handshake with master.")
        else:
            logging.error("Handshake with master failed.")

    except asyncio.TimeoutError:
        logging.error("Timeout error when connecting to master.")
    except (ConnectionRefusedError, ConnectionResetError):
        logging.error("Connection to master refused or reset.")
    except Exception as e:
        logging.error(f"Error connecting to master: {e}")
    finally:
        if writer:
            writer.close()
            await writer.wait_closed()


async def perform_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, port: int
) -> bool:
    # Ping command
    if not await send_command_and_expect(reader, writer, ["PING"], "+PONG"):
        return False

    # Configure listening port
    if not await send_command_and_expect(
        reader, writer, ["REPLCONF", "listening-port", str(port)], "+OK"
    ):
        return False

    # Capability negotiation
    if not await send_command_and_expect(
        reader, writer, ["REPLCONF", "capa", "npsync2"], "+OK"
    ):
        return False

    # Attempt PSYNC
    run_id = "?"
    offset = "-1"
    if not await send_command_and_expect(
        reader, writer, ["PSYNC", run_id, offset], None
    ):
        return False

    return True


async def send_command_and_expect(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    command: list[str],
    expected_response: str | None = None,
) -> str:
    writer.write(RESPEncoder.encode_array(*command))
    await writer.drain()

    data = await reader.read(1024)
    response = data.decode().strip()

    if expected_response and response != expected_response.strip():
        logging.warning(f"Unexpected response from master: {response}")
        return ""

    return response
