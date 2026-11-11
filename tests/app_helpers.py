import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from client.client import CollabClient
from server.server import CollabServer

USERS = str(Path(__file__).resolve().parents[1] / "server/data/users.json")


async def wait_until(predicate, timeout=10):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("condition timed out")
        await asyncio.sleep(0.01)


async def event(client, name, predicate=lambda ev: True, timeout=15):
    async def receive():
        while True:
            ev = await client.events.get()
            if ev["event"] == name and predicate(ev):
                return ev

    return await asyncio.wait_for(receive(), timeout)


@asynccontextmanager
async def session(tmp_path, count=2):
    server = CollabServer("127.0.0.1", 0, USERS)
    await server.start()
    clients = [
        CollabClient(server.endpoint.local_addr, str(tmp_path / str(i)))
        for i in range(count)
    ]
    try:
        for client, name in zip(clients, ("alice", "bob", "carol")):
            await client.start()
            assert (await client.login(name, name + "123"))[0]
        yield server, clients
    finally:
        await asyncio.gather(*(c.stop() for c in clients), return_exceptions=True)
        await server.stop()
