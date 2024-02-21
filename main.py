import argparse
import asyncio
import logging

from app.config import Config

# main.py
from app.database import RedisDatabase
from app.server import start_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="The port to listen on")
    parser.add_argument(
        "--replicaof",
        nargs=2,
        metavar=("MASTER_HOST", "MASTER_PORT"),
        help="Master host and master port for the replica.",
    )
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
    master_host, master_port = args.replicaof or (None, None)
    config = Config(
        dir=args.dir,
        dbfilename=args.dbfilename,
        master_host=master_host,
        master_port=master_port,
    )

    db = RedisDatabase(config=config)

    host = "127.0.0.1"
    port = args.port or 6379
    await start_server(host, port, db)


if __name__ == "__main__":
    asyncio.run(main())
