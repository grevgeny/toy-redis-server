import asyncio
import logging

from app.command_handler import CommandHandler
from app.database import RedisDatabase


async def handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, db: RedisDatabase
):
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
            arg for arg in data.decode().split("\r\n")[:-1] if arg[0] not in "*$"
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


async def main(host: str, port: int):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db = RedisDatabase()
    server = await asyncio.start_server(
        lambda r, w: handle_connection(r, w, db), host, port
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 6379
    asyncio.run(main(HOST, PORT))
