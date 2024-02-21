# app/server.py
import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase


async def handle_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    db: RedisDatabase,
) -> None:
    command_handler = CommandHandler(db)
    addr = writer.get_extra_info("peername")
    logging.info(f"Connected by {addr}")

    while True:
        try:
            data = await reader.read(1024)
        except ConnectionError:
            logging.error(f"Client suddenly closed while receiving from {addr}")
            break
        if not data:
            break

        command = [
            arg.lower() for arg in data.decode().split("\r\n")[:-1] if arg[0] not in "$"
        ]
        response = await command_handler.handle_command(command)
        try:
            writer.write(response)
            await writer.drain()
        except ConnectionError:
            logging.error("Client suddenly closed, cannot send")
            break

    writer.close()
    logging.info(f"Disconnected by {addr}")


async def start_server(host: str, port: int, db: RedisDatabase):
    async def on_client_connect(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        await handle_connection(reader, writer, db)

    server = await asyncio.start_server(on_client_connect, host, port)
    logging.info(f"Server started on {host}:{port}")

    async with server:
        await server.serve_forever()
