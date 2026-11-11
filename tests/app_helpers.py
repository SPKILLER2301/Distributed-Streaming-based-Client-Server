import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
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
    server = CollabServer("127.0.0.1", 0, USERS, tcp_port=0)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(stop_app, server)
        await server.start()
        clients = [
            CollabClient(server._bootstrap.local_addr, str(tmp_path / str(i)))
            for i in range(count)
        ]
        for client in clients:
            cleanup.push_async_callback(stop_app, client)
        for client, name in zip(clients, ("alice", "bob", "carol")):
            await client.start()
            assert (await client.login(name, name + "123"))[0]
        yield server, clients


async def stop_app(app):
    link, app.endpoint.link = app.endpoint.link, None
    if link is not None:
        link.close()
    await app.stop()
