import argparse
import asyncio
import logging

from toy_redis_server.server.master import MasterServer
from toy_redis_server.server.replica import ReplicaServer


def parse_args() -> argparse.Namespace:
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
        "--replicaof", nargs=2, help="Master host and master port for the replica."
    )

    return parser.parse_args()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = parse_args()

    if args.replicaof:
        server = ReplicaServer(
            host=args.host,
            port=args.port,
            master_host=args.replicaof[0],
            master_port=args.replicaof[1],
        )
    else:
        server = MasterServer(
            host=args.host,
            port=args.port,
            dir=args.dir,
            filename=args.dbfilename,
        )

    try:
        await server.start()
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
