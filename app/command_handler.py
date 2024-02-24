from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from app.database import RedisDatabase


@dataclass
class Command(ABC):
    database: RedisDatabase
    args: list[str] = field(default_factory=list)

    @abstractmethod
    async def execute(self) -> bytes:
        pass


class PingCommand(Command):
    async def execute(self) -> bytes:
        return b"+PONG\r\n"


class EchoCommand(Command):
    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"
        message = " ".join(self.args)
        return f"${len(message)}\r\n{message}\r\n".encode()


class SetCommand(Command):
    async def execute(self) -> bytes:
        if len(self.args) < 2:
            return b"-ERR wrong number of arguments for 'set' command\r\n"
        key, value = self.args[0], self.args[1]
        expiry_ms = int(self.args[3]) if len(self.args) > 3 else None
        await self.database.set(key, value, expiry_ms)
        return b"+OK\r\n"


class GetCommand(Command):
    async def execute(self) -> bytes:
        if len(self.args) != 1:
            return b"-ERR wrong number of arguments for 'get' command\r\n"
        key = self.args[0]
        value = await self.database.get(key)
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode()


class DeleteCommand(Command):
    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'del' command\r\n"
        total_deleted = sum([await self.database.delete(key) for key in self.args])
        return f":{total_deleted}\r\n".encode()


class ConfigCommand(Command):
    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'config get' command\r\n"
        if self.args[0] == "get":
            key = self.args[1]
            if key == "dir":
                value = self.database.config.rdb_dir
            elif key == "dbfilename":
                value = self.database.config.rdb_filename
            else:
                return b"-ERR unknown config key\r\n"

            if value:
                return f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n".encode()
            else:
                return b"$-1\r\n"
        else:
            return b"-ERR wrong arguments for 'config' command\r\n"


class KeysCommand(Command):
    async def execute(self) -> bytes:
        if not self.args or self.args[0] != "*":
            return b"-ERR wrong number of arguments for 'keys' command\r\n"
        keys = await self.database.keys()
        n = len(keys)
        keys_array = [f"${len(key)}\r\n{key}\r\n" for key in keys]
        return f"*{n}\r\n{''.join(keys_array)}".encode()


class InfoCommand(Command):
    async def execute(self) -> bytes:
        if not self.args or self.args[0] != "replication":
            return b"-ERR wrong arguments for 'info' command provided\r\n"

        role = "role:master" if not self.database.config.master_host else "role:slave"
        master_replid = f"master_replid:{self.database.config.master_replid}"
        master_repl_offset = (
            f"master_repl_offset:{self.database.config.master_repl_offset}"
        )

        response_parts = [role, master_replid, master_repl_offset]
        response = "\r\n".join(response_parts)

        return f"${len(response)}\r\n{response}\r\n".encode()


async def create_command(
    command_name: str, args: list[str], database: RedisDatabase
) -> Command:
    match command_name:
        case "ping":
            return PingCommand(database)
        case "echo":
            return EchoCommand(database, args)
        case "set":
            return SetCommand(database, args)
        case "get":
            return GetCommand(database, args)
        case "delete":
            return DeleteCommand(database, args)
        case "config":
            return ConfigCommand(database, args)
        case "keys":
            return KeysCommand(database, args)
        case "info":
            return InfoCommand(database, args)
        case _:
            raise ValueError(f"Unknown command '{command_name}'")


class CommandHandler:
    def __init__(self, database: RedisDatabase) -> None:
        self.database = database

    async def handle_command(self, raw_command: list[str]) -> bytes:
        if not raw_command:
            return b"-ERR no command provided\r\n"

        command_name, *args = raw_command[1:]
        try:
            command = await create_command(command_name, args, self.database)
            return await command.execute()
        except ValueError:
            return f"-ERR unknown command '{command_name}'\r\n".encode()
