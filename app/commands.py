from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.rdb.helpers import get_empty_rdb
from app.redis_config import RedisConfig
from app.resp.encoder import RESPEncoder
from app.storage import Storage


@dataclass
class Command(ABC):
    @abstractmethod
    async def execute(self) -> bytes:
        pass


@dataclass
class PingCommand(Command):
    async def execute(self) -> bytes:
        return RESPEncoder.encode_simple_string("PONG")


@dataclass
class EchoCommand(Command):
    args: list[str]

    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'echo' command\r\n"

        return RESPEncoder.encode_bulk_string(" ".join(self.args))


@dataclass
class SetCommand(Command):
    storage: Storage
    args: list[str]

    async def execute(self) -> bytes:
        if len(self.args) < 2:
            return b"-ERR wrong number of arguments for 'set' command\r\n"

        key, value = self.args[0], self.args[1]
        expiry_ms = int(self.args[3]) if len(self.args) > 3 else None

        await self.storage.set(key, value, expiry_ms)

        return RESPEncoder.encode_simple_string("OK")


@dataclass
class GetCommand(Command):
    storage: Storage
    args: list[str]

    async def execute(self) -> bytes:
        if len(self.args) != 1:
            return b"-ERR wrong number of arguments for 'get' command\r\n"

        key = self.args[0]
        value = await self.storage.get(key)

        if value is None:
            return RESPEncoder.encode_null()

        return RESPEncoder.encode_bulk_string(value)


@dataclass
class DeleteCommand(Command):
    storage: Storage
    args: list[str]

    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'del' command\r\n"

        total_deleted = sum([await self.storage.delete(key) for key in self.args])

        return RESPEncoder.encode_integer(total_deleted)


@dataclass
class ConfigCommand(Command):
    config: RedisConfig
    args: list[str]

    async def execute(self) -> bytes:
        if not self.args:
            return b"-ERR wrong number of arguments for 'config get' command\r\n"
        if self.args[0] == "get":
            key = self.args[1]
            if key == "dir":
                value = self.config.rdb_dir
            elif key == "dbfilename":
                value = self.config.rdb_filename
            else:
                return b"-ERR unknown config key\r\n"

            if value:
                return RESPEncoder.encode_array(key, value)
            else:
                return RESPEncoder.encode_null()
        else:
            return b"-ERR wrong arguments for 'config' command\r\n"


@dataclass
class KeysCommand(Command):
    storage: Storage
    arg: str

    async def execute(self) -> bytes:
        if self.arg != "*":
            return b"-ERR wrong argument for 'keys' command\r\n"

        keys = await self.storage.keys()

        return RESPEncoder.encode_array(*keys)


@dataclass
class InfoCommand(Command):
    config: RedisConfig
    args: list[str]

    async def execute(self) -> bytes:
        if not self.args or self.args[0] != "replication":
            return b"-ERR wrong arguments for 'info' command provided\r\n"

        role = f"role:{self.config.role.value}"
        master_replid = f"master_replid:{self.config.master_replid}"
        master_repl_offset = f"master_repl_offset:{self.config.master_repl_offset}"

        response_parts = [role, master_replid, master_repl_offset]
        response = "\n".join(response_parts)

        return RESPEncoder.encode_bulk_string(response)


@dataclass
class ReplconfCommand(Command):
    async def execute(self) -> bytes:
        return RESPEncoder.encode_simple_string("OK")


@dataclass
class PsyncCommand(Command):
    config: RedisConfig

    async def execute(self) -> bytes:
        full_resync = RESPEncoder.encode_simple_string(
            f"FULLRESYNC {self.config.master_replid} {self.config.master_repl_offset}"
        )
        empty_rdb = get_empty_rdb()

        return full_resync + f"${len(empty_rdb)}\r\n".encode() + empty_rdb


@dataclass
class CommandUnknown(Command):
    name: str

    async def execute(self) -> bytes:
        return f"-ERR unknown command {self.name}".encode()


async def parse_command(
    storage: Storage,
    config: RedisConfig,
    raw_command: list[str],
) -> Command:
    if not raw_command:
        return CommandUnknown(name="")

    normalized_command = [raw_command[0].lower(), *raw_command[1:]]

    match normalized_command:
        case ["ping"]:
            return PingCommand()
        case ["echo", *args]:
            return EchoCommand(args)
        case ["set", *args]:
            return SetCommand(storage, args)
        case ["get", *args]:
            return GetCommand(storage, args)
        case ["del", *args]:
            return DeleteCommand(storage, args)
        case ["config", *args]:
            return ConfigCommand(config, args)
        case ["keys", arg]:
            return KeysCommand(storage, arg)
        case ["info", *args]:
            return InfoCommand(config, args)
        case ["replconf", *args]:
            return ReplconfCommand()
        case ["psync", *args]:
            return PsyncCommand(config)
        case _:
            return CommandUnknown(name=raw_command[0])
