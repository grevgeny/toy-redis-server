import argparse
import asyncio
import logging

from app.redis_config import RedisConfig, Role
from app.server import MasterRedisServer, SlaveRedisServer


def parse_args() -> RedisConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="The host address to bind"
    )
    parser.add_argument("--port", type=int, default=6379, help="The port to listen on")
    parser.add_argument(
        "--dir", type=str, help="The directory where RDB files are stored"
    )
    parser.add_argument("--dbfilename", type=str, help="The name of the RDB file")
    parser.add_argument(
        "--replicaof",
        nargs=2,
        metavar=("MASTER_HOST", "MASTER_PORT"),
        help="Master host and master port for the replica.",
    )
    args = parser.parse_args()
    master_host, master_port = args.replicaof if args.replicaof else (None, None)
    role = Role.SLAVE if master_host else Role.MASTER

    return RedisConfig(
        host=args.host,
        port=args.port,
        role=role,
        rdb_dir=args.dir,
        rdb_filename=args.dbfilename,
        master_host=master_host,
        master_port=int(master_port) if master_port else None,
    )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = parse_args()

    if config.role == Role.MASTER:
        logging.info("Starting as master")
        server = MasterRedisServer.from_config(config)
    else:
        logging.info("Starting as slave")
        server = await SlaveRedisServer.from_config(config)

    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
