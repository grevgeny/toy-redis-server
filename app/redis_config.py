from dataclasses import dataclass


@dataclass
class RedisConfig:
    rdb_dir: str | None
    rdb_filename: str | None
    master_host: str | None
    master_port: int | None
    port: int = 6379
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0

    def __post_init__(self) -> None:
        if self.master_port:
            self.master_port = int(self.master_port)
