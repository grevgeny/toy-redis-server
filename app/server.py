import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase
from app.resp.decoder import RESPDecoder


async def start_server(host: str, port: int, db: RedisDatabase):
    async def on_client_connect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        await handle_connection(reader, writer, db)

    server = await asyncio.start_server(on_client_connect, host, port)
    logging.info(f"Server started on {host}:{port}\n")

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
