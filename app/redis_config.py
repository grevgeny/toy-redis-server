import secrets
from dataclasses import dataclass, field
from enum import Enum, auto


class Role(Enum):
    MASTER = auto()
    SLAVE = auto()


@dataclass
class RedisConfig:
    host: str
    port: int
    rdb_dir: str | None
    rdb_filename: str | None
    master_host: str | None
    master_port: int | None
    role: Role
    master_replid: str | None = field(default=None, init=False)
    master_repl_offset: int | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self.role == Role.MASTER:
            self.master_replid = secrets.token_hex(40)
            self.master_repl_offset = 0
