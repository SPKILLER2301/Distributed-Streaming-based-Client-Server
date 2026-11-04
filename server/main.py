"""
Điểm chạy server.      [Người phụ trách: B]
    python -m server.main --port 9000 [--host 0.0.0.0] [-v]
"""
import argparse
import asyncio
import logging
from pathlib import Path

from common import constants as C

from .server import CollabServer


def main() -> None:
    p = argparse.ArgumentParser(description="Collab App server (UDP)")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=C.DEFAULT_SERVER_PORT)
    p.add_argument("--users", default=str(Path(__file__).parent / "data" / "users.json"))
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        asyncio.run(CollabServer(args.host, args.port, args.users).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
