import argparse
import asyncio
import logging

# main.py
from app.config import RedisConfig
from app.database import RedisDatabase
from app.server import start_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Toy Redis Server with RDB Persistence"
    )
    parser.add_argument(
        "--dir", type=str, help="The directory where RDB files are stored"
    )
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    return parser.parse_args()


async def main() -> None:
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Parse command-line arguments
    args = parse_args()
    config = RedisConfig(dir=args.dir, dbfilename=args.dbfilename)

    # Setup Redis database
    db = RedisDatabase()

    # Start Redis server
    host, port = "127.0.0.1", 6379
    await start_server(host, port, db, config)


if __name__ == "__main__":
    asyncio.run(main())
