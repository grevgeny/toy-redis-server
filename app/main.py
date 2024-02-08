import asyncio
import datetime
import logging
from typing import Any


class RedisDatabase:
    def __init__(self):
        self.data: dict[str, tuple[Any, datetime.datetime | None]] = {}

    async def set(self, key: str, value: Any, expiry_ms: int | None = None) -> None:
        """
        Set the value of a key with an optional expiry time in milliseconds.
        """
        expiry = (
            datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(milliseconds=expiry_ms)
            if expiry_ms
            else None
        )
        self.data[key] = (value, expiry)

    async def get(self, key: str) -> Any | None:
        """
        Get the value of a key if it hasn't expired.
        """
        value, expiry = self.data.get(key, (None, None))
        if expiry and expiry < datetime.datetime.now(datetime.timezone.utc):
            await self.delete(key)  # Expire the key
            return None
        return value

    async def delete(self, key: str) -> None:
        """
        Delete a key from the database.
        """
        if key in self.data:
            del self.data[key]


class CommandHandler:
    def __init__(self, database: RedisDatabase) -> None:
        self.database = database

    async def handle_command(self, command: list[str]) -> bytes:
        """
        Dispatch the appropriate command to its handler and return the response.
        """
        if not command:
            return b"-ERR no command provided\r\n"

        cmd = command[0].lower()
        args = command[1:]

        if cmd == "ping":
            return self.ping()
        elif cmd == "echo":
            return self.echo(args)
        elif cmd == "set":
            return await self.set(args)
        elif cmd == "get":
            return await self.get(args)
        elif cmd == "del":
            return await self.delete(args)
        else:
            return f"-ERR unknown command '{command[0]}'\r\n".encode()

    def ping(self) -> bytes:
        """
        Simple PING command.
        """
        return b"+PONG\r\n"

    def echo(self, args: list[str]) -> bytes:
        """
        ECHO command to return the message sent to it.
        """
        if not args:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"
        message = " ".join(args)
        return f"${len(message)}\r\n{message}\r\n".encode()

    async def set(self, args: list[str]) -> bytes:
        """
        SET command to store a value.
        """
        if len(args) < 2:
            return b"-ERR wrong number of arguments for 'set' command\r\n"
        key, value = args[0], args[1]
        expiry_ms = int(args[3]) if len(args) > 3 else None
        await self.database.set(key, value, expiry_ms)
        return b"+OK\r\n"

    async def get(self, args: list[str]) -> bytes:
        """
        GET command to retrieve a value.
        """
        if len(args) != 1:
            return b"-ERR wrong number of arguments for 'get' command\r\n"
        key = args[0]
        value = await self.database.get(key)
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode()

    async def delete(self, args: list[str]) -> bytes:
        """
        DEL command to delete one or more keys.
        """
        if not args:
            return b"-ERR wrong number of arguments for 'del' command\r\n"
        for key in args:
            await self.database.delete(key)
        return f":{len(args)}\r\n".encode()


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
