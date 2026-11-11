"""Kiểm thử Registry/Auth và luồng đăng nhập + chat đầu cuối.   [B]"""

import asyncio
from contextlib import AsyncExitStack
from pathlib import Path


from client.client import CollabClient
from server.auth import Auth
from server.registry import Registry
from server.server import CollabServer

USERS = str(Path(__file__).resolve().parents[1] / "server" / "data" / "users.json")


def test_registry_basic():
    r = Registry()
    assert r.add("alice", 1, ("1.2.3.4", 5))
    assert not r.add("alice", 2, ("1.2.3.4", 6))  # trùng tên
    assert r.usernames() == ["alice"]
    assert r.remove_by_conn(1).username == "alice"
    assert r.usernames() == []


def test_auth():
    a = Auth(USERS)
    assert a.verify("alice", "alice123")
    assert not a.verify("alice", "sai")
    assert not a.verify("khong_ton_tai", "x")


async def _wait(cond, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while not cond():
        assert asyncio.get_running_loop().time() < end, "hết thời gian chờ"
        await asyncio.sleep(0.01)


async def _receive_chat(client, timeout=5.0):
    # Connection/user events can already fill the queue before chat is sent.
    # Use one deadline for the whole wait, including unrelated events.
    async with asyncio.timeout(timeout):
        while True:
            event = await client.events.get()
            if event.get("event") == "chat":
                return event


async def _stop_app(app):
    link, app.endpoint.link = app.endpoint.link, None
    if link is not None:
        link.close()
    await app.stop()


def test_login_broadcast_private_end_to_end():
    async def run():
        from tests.lossy_socket import LossyLink

        server = CollabServer("127.0.0.1", 0, USERS, tcp_port=0)
        cleanup = AsyncExitStack()
        cleanup.push_async_callback(_stop_app, server)
        try:
            await server.start()
            addr = server._bootstrap.local_addr
            a, b = CollabClient(addr), CollabClient(addr)
            # Register before start so partial startup/assertion failures
            # still close both clients, then the server.
            cleanup.push_async_callback(_stop_app, b)
            cleanup.push_async_callback(_stop_app, a)
            await a.start(); await b.start()
            assert (await a.login("alice", "alice123"))[0]
            assert not (await b.login("bob", "sai"))[0]
            assert (await b.login("bob", "bob123"))[0]
            await _wait(lambda: "bob" in a.users)              # a thấy bob online
            # Keep B's delayed-delivery regression with login/user events queued.
            for endpoint in (server.endpoint, a.endpoint, b.endpoint):
                endpoint.link = LossyLink(delay=0.025)
            b.send_chat("chào cả nhà")
            a.send_chat("riêng", to="bob")
            broadcast, private = await asyncio.gather(_receive_chat(a), _receive_chat(b))
            assert broadcast["from"] == "bob"
            assert broadcast["text"] == "chào cả nhà"
            assert broadcast["private"] is False
            assert private["from"] == "alice"
            assert private["text"] == "riêng"
            assert private["private"] is True
        finally:
            await cleanup.aclose()
    asyncio.run(run())


def test_file_transfer_end_to_end(tmp_path):
    async def run():
        server = CollabServer("127.0.0.1", 0, USERS, tcp_port=0)
        async with AsyncExitStack() as cleanup:
            cleanup.push_async_callback(_stop_app, server)
            await server.start()
            addr = server._bootstrap.local_addr
            a = CollabClient(addr, str(tmp_path / "dl_a"))
            b = CollabClient(addr, str(tmp_path / "dl_b"))
            cleanup.push_async_callback(_stop_app, b)
            cleanup.push_async_callback(_stop_app, a)
            await a.start()
            await b.start()
            assert (await a.login("alice", "alice123"))[0]
            assert (await b.login("bob", "bob123"))[0]
            src = tmp_path / "data.bin"
            payload = bytes(range(256)) * 1500  # ~384 KB
            src.write_bytes(payload)
            file_id = a.send_file(str(src), "bob")
            await _wait(lambda: file_id in b._incoming_offers)
            b.accept_file(file_id)
            from tests.app_helpers import event

            received, sent = await asyncio.gather(
                event(b, "file_done", lambda ev: ev["file_id"] == file_id),
                event(a, "file_done", lambda ev: ev["file_id"] == file_id),
            )
            assert received["ok"] and sent["ok"]
            assert (tmp_path / "dl_b" / "data.bin").read_bytes() == payload
            assert not server.router.transfers and not a._outgoing and not b._receivers

    asyncio.run(run())


def test_client_crash_is_cleaned_up(tmp_path, monkeypatch):
    from common import constants as C
    from tests.app_helpers import session, event, wait_until

    monkeypatch.setattr(C, "PEER_TIMEOUT", 0.5)
    monkeypatch.setattr(C, "HEARTBEAT_INTERVAL", 0.1)

    async def run():
        async with session(tmp_path) as (server, (a, b)):
            a._stopping = True  # crashed process cannot run a reconnect supervisor
            a.endpoint._transport.close()  # test-only fault injection: no LOGOUT/CLOSE
            await wait_until(lambda: server.registry.get_by_name("alice") is None, 3)
            assert (await event(b, "users", lambda ev: "alice" not in ev["users"]))[
                "users"
            ] == ["bob"]

    asyncio.run(run())
