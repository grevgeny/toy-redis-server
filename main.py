import argparse
import asyncio
import logging

# main.py
from app.database import RedisDatabase
from app.exceptions import ReplicationInitializationError
from app.redis_config import RedisConfig, Role
from app.server import start_server


def create_redis_config_from_args() -> RedisConfig:
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


async def main(config: RedisConfig) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db = None
    if config.role == Role.SLAVE:
        try:
            db = await RedisDatabase.init_slave(config=config)
        except ReplicationInitializationError as e:
            logging.error(f"Failed to start as a slave: {e}")
    else:
        db = RedisDatabase.init_master(config=config)

    if db:
        try:
            await start_server(host=config.host, port=config.port, db=db)
        finally:
            await db.close()
    else:
        logging.error("Database initialization failed. Exiting.")


if __name__ == "__main__":
    config = create_redis_config_from_args()
    asyncio.run(main(config))
