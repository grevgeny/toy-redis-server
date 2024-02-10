from dataclasses import dataclass


@dataclass
class RedisConfig:
    dir: str
    dbfilename: str
