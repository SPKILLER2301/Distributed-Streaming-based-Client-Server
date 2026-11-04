"""
Điểm chạy client + giao diện web cục bộ.      [Người phụ trách: B]
    python -m client.main --server 127.0.0.1:9000 --ui-port 8080
Sau đó mở http://127.0.0.1:8080 trong trình duyệt.
"""
import argparse
import asyncio
import logging

from common import constants as C

from .client import CollabClient
from .web_ui.http_server import WebUI


async def run(server: str, ui_port: int) -> None:
    host, port = server.rsplit(":", 1)
    client = CollabClient((host, int(port)))
    await client.start()
    ui = WebUI(client, ui_port)
    await ui.start()
    print(f"Mở giao diện: http://127.0.0.1:{ui_port}")
    await asyncio.Event().wait()


def main() -> None:
    p = argparse.ArgumentParser(description="Collab App client")
    p.add_argument("--server", default=f"{C.DEFAULT_SERVER_HOST}:{C.DEFAULT_SERVER_PORT}")
    p.add_argument("--ui-port", type=int, default=C.DEFAULT_UI_PORT)
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    try:
        asyncio.run(run(args.server, args.ui_port))
    except (KeyboardInterrupt, ConnectionError) as exc:
        if isinstance(exc, ConnectionError):
            print("Lỗi:", exc)


if __name__ == "__main__":
    main()
