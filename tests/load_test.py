"""Assert concurrent login and exact broadcast delivery, with a login barrier.
python -m tests.load_test --server 127.0.0.1:9000 --clients 20
"""

import argparse
import asyncio
import json
import time
import tracemalloc
from client.client import CollabClient


async def run_load(addr, n=20, timeout=20):
    if not 2 <= n <= 20:
        raise ValueError("clients must be 2..20")
    clients = [CollabClient(addr) for _ in range(n)]
    started = time.monotonic()
    tracemalloc.start()
    try:
        await asyncio.wait_for(asyncio.gather(*(c.start() for c in clients)), timeout)
        names = [f"user_{i:03d}" for i in range(1, n + 1)]
        outcomes = await asyncio.wait_for(
            asyncio.gather(
                *(c.login(name, name + "123") for c, name in zip(clients, names))
            ),
            timeout,
        )
        if not all(ok for ok, _ in outcomes):
            raise AssertionError(f"login failures: {outcomes}")
        login_seconds = time.monotonic() - started
        # All logins have finished; drain announcements before any broadcast is sent.
        for c in clients:
            while not c.events.empty():
                c.events.get_nowait()
        for c, name in zip(clients, names):
            c.send_chat(f"load:{name}")

        async def collect(c):
            received = []
            deadline = asyncio.get_running_loop().time() + timeout
            while len(received) < n - 1:
                ev = await asyncio.wait_for(
                    c.events.get(), max(0, deadline - asyncio.get_running_loop().time())
                )
                if ev["event"] == "chat":
                    received.append((ev["from"], ev["text"]))
            return received

        result = await asyncio.gather(*(collect(c) for c in clients))
        await asyncio.sleep(0.3)  # bounded observation for late duplicates
        for c, own, received in zip(clients, names, result):
            while not c.events.empty():
                ev = c.events.get_nowait()
                if ev["event"] == "chat":
                    received.append((ev["from"], ev["text"]))
            expected = {(name, f"load:{name}") for name in names if name != own}
            assert set(received) == expected, f"{own}: missing/unexpected messages"
            assert len(received) == len(expected), f"{own}: duplicate messages"
        return {
            "clients": n,
            "expected": n * (n - 1),
            "received": sum(map(len, result)),
            "duplicates": 0,
            "login_seconds": round(login_seconds, 3),
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "peak_python_bytes": tracemalloc.get_traced_memory()[1],
            "memory_scope": "load generator only; not server RSS",
        }
    finally:
        await asyncio.gather(*(c.stop() for c in clients), return_exceptions=True)
        tracemalloc.stop()


async def main(server, n):
    host, port = server.rsplit(":", 1)
    print(json.dumps(await run_load((host, int(port)), n), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="127.0.0.1:9000")
    parser.add_argument("--clients", type=int, default=20)
    args = parser.parse_args()
    # Assertion/timeout exceptions intentionally produce non-zero process exit status.
    asyncio.run(main(args.server, args.clients))
