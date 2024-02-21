from dataclasses import dataclass


@dataclass
class Config:
    dir: str
    dbfilename: str
    master_host: str
    master_port: int
