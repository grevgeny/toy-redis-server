from app.config import RedisConfig
from app.database import RedisDatabase


class CommandHandler:
    def __init__(self, database: RedisDatabase, config: RedisConfig) -> None:
        self.database = database
        self.config = config

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
        if cmd == "config" and args[0].lower() == "get":
            return self.config_get(args[1:])
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

    def config_get(self, args: list[str]) -> bytes:
        if not args:
            return b"-ERR wrong number of arguments for 'config get' command\r\n"
        key = args[0].lower()
        if key == "dir":
            value = self.config.dir
        elif key == "dbfilename":
            value = self.config.dbfilename
        else:
            return b"-ERR unknown config key\r\n"
        return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n".encode()
