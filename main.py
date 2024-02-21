import argparse
import asyncio
import logging
import os

# main.py
from app.config import RedisConfig
from app.database import RedisDatabase
from app.server import start_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="The port to listen on")
    parser.add_argument(
        "--dir", type=str, help="The directory where RDB files are stored"
    )
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    return parser.parse_args()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = parse_args()
    config = RedisConfig(dir=args.dir, dbfilename=args.dbfilename)

    rdb_file_path = (
        os.path.join(config.dir, config.dbfilename)
        if config.dir and config.dbfilename
        else None
    )
    db = RedisDatabase(rdb_file_path=rdb_file_path)

    host = "127.0.0.1"
    port = args.port or 6379
    await start_server(host, port, db, config)


if __name__ == "__main__":
    asyncio.run(main())
