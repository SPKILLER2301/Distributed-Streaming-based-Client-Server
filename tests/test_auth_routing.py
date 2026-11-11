"""Kiểm thử Registry/Auth và luồng đăng nhập + chat đầu cuối.   [B]"""

import asyncio
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


async def _wait_chat(client, *, text, sender, private, timeout=5.0):
    """Wait for the expected chat, ignoring login/user-list events.

    Queue length cannot prove that a particular network message has arrived.
    The timeout covers the complete wait, including unrelated events.
    """
    async def receive():
        while True:
            event = await client.events.get()
            if (
                event.get("event") == "chat"
                and event.get("text") == text
                and event.get("from") == sender
                and event.get("private") is private
            ):
                return event

    return await asyncio.wait_for(receive(), timeout)


def test_login_broadcast_private_end_to_end():
    async def run():
        from tests.lossy_socket import LossyLink

        server = CollabServer("127.0.0.1", 0, USERS)
        await server.endpoint.start(("127.0.0.1", 0))
        addr = ("127.0.0.1", server.endpoint.local_addr[1])
        a, b = CollabClient(addr), CollabClient(addr)
        try:
            await a.start()
            await b.start()
            assert (await a.login("alice", "alice123"))[0]
            assert not (await b.login("bob", "sai"))[0]
            assert (await b.login("bob", "bob123"))[0]
            await _wait(lambda: "bob" in a.users)

            # Exercise delayed delivery while login/user events remain queued.
            # This exposes the old qsize-based wait without requiring Windows.
            for endpoint in (server.endpoint, a.endpoint, b.endpoint):
                endpoint.link = LossyLink(delay=0.025)
            b.send_chat("chào cả nhà")
            a.send_chat("riêng", to="bob")
            broadcast, direct = await asyncio.gather(
                _wait_chat(a, text="chào cả nhà", sender="bob", private=False),
                _wait_chat(b, text="riêng", sender="alice", private=True),
            )
            assert broadcast["text"] == "chào cả nhà"
            assert direct["text"] == "riêng"
        finally:
            await asyncio.gather(a.stop(), b.stop(), return_exceptions=True)
            await server.stop()

    asyncio.run(run())


def test_file_transfer_end_to_end(tmp_path):
    async def run():
        server = CollabServer("127.0.0.1", 0, USERS)
        await server.endpoint.start(("127.0.0.1", 0))
        addr = ("127.0.0.1", server.endpoint.local_addr[1])
        a = CollabClient(addr, str(tmp_path / "dl_a"))
        b = CollabClient(addr, str(tmp_path / "dl_b"))
        await a.start()
        await b.start()
        await a.login("alice", "alice123")
        await b.login("bob", "bob123")
        src = tmp_path / "data.bin"
        payload = bytes(range(256)) * 1500  # ~384 KB
        src.write_bytes(payload)
        file_id = a.send_file(str(src), "bob")
        await _wait(lambda: file_id in b._incoming_offers)
        b.accept_file(file_id)
        await _wait(
            lambda: any(e["event"] == "file_done" for e in list(b.events._queue)),
            timeout=15,
        )
        assert (tmp_path / "dl_b" / "data.bin").read_bytes() == payload
        await a.stop()
        await b.stop()
        await server.endpoint.stop()

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
