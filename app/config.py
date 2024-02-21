from dataclasses import dataclass


@dataclass
class Config:
    rdb_dir: str | None
    rdb_filename: str | None
    master_host: str | None
    master_port: int | None
    port: int = 6379

    def __post_init__(self) -> None:
        if self.master_port:
            self.master_port = int(self.master_port)
