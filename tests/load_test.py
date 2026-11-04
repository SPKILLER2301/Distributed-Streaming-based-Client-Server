"""
Kiểm thử tải: N client đăng nhập cùng lúc và chat broadcast.   [Người phụ trách: B]  (Bước 7, 10)

Chạy (server đã bật):   python -m tests.load_test --server 127.0.0.1:9000 --clients 20
Dùng các tài khoản user_001..user_020 (mật khẩu = tên + "123") đã có sẵn trong server/data/users.json.
"""
import argparse
import asyncio
import time

from client.client import CollabClient


async def one_client(addr, i: int, results: dict) -> None:
    name = f"user_{i:03d}"
    c = CollabClient(addr, download_dir=f"downloads/{name}")
    await c.start()
    ok, reason = await c.login(name, name + "123")
    results[name] = ok or reason
    if ok:
        c.send_chat(f"xin chào từ {name}")
        await asyncio.sleep(2.0)
        got = sum(1 for e in list(c.events._queue) if e["event"] == "chat")
        results[name] = f"ok, nhận {got} tin broadcast"
    await c.stop()


async def main(server: str, n: int) -> None:
    host, port = server.rsplit(":", 1)
    results: dict = {}
    t0 = time.time()
    await asyncio.gather(*(one_client((host, int(port)), i, results) for i in range(1, n + 1)))
    print(f"{n} client trong {time.time() - t0:.1f}s")
    for k, v in sorted(results.items()):
        print(" ", k, v)
    # TODO(B): in thống kê: số client thành công, thời gian đăng nhập trung bình, số tin bị mất


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--server", default="127.0.0.1:9000")
    p.add_argument("--clients", type=int, default=20)
    a = p.parse_args()
    asyncio.run(main(a.server, a.clients))
