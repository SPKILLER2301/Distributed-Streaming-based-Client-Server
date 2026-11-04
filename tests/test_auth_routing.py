"""Kiểm thử Registry/Auth và luồng đăng nhập + chat đầu cuối.   [B]"""
import asyncio
from pathlib import Path

import pytest

from client.client import CollabClient
from server.auth import Auth
from server.registry import Registry
from server.server import CollabServer

USERS = str(Path(__file__).resolve().parents[1] / "server" / "data" / "users.json")


def test_registry_basic():
    r = Registry()
    assert r.add("alice", 1, ("1.2.3.4", 5))
    assert not r.add("alice", 2, ("1.2.3.4", 6))              # trùng tên
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


async def _drain(client):
    out = []
    while not client.events.empty():
        out.append(client.events.get_nowait())
    return out


def test_login_broadcast_private_end_to_end():
    async def run():
        server = CollabServer("127.0.0.1", 0, USERS)
        await server.endpoint.start(("127.0.0.1", 0))
        addr = ("127.0.0.1", server.endpoint.local_addr[1])
        a, b = CollabClient(addr), CollabClient(addr)
        await a.start(); await b.start()
        assert (await a.login("alice", "alice123"))[0]
        assert not (await b.login("bob", "sai"))[0]
        assert (await b.login("bob", "bob123"))[0]
        await _wait(lambda: "bob" in a.users)                  # a thấy bob online
        b.send_chat("chào cả nhà")
        a.send_chat("riêng", to="bob")
        await _wait(lambda: a.events.qsize() >= 3 and b.events.qsize() >= 3)
        ev_a, ev_b = await _drain(a), await _drain(b)
        assert any(e["event"] == "chat" and e["text"] == "chào cả nhà" and e["from"] == "bob" for e in ev_a)
        assert any(e["event"] == "chat" and e["private"] and e["from"] == "alice" for e in ev_b)
        await a.stop(); await b.stop(); await server.endpoint.stop()
    asyncio.run(run())


def test_file_transfer_end_to_end(tmp_path):
    async def run():
        server = CollabServer("127.0.0.1", 0, USERS)
        await server.endpoint.start(("127.0.0.1", 0))
        addr = ("127.0.0.1", server.endpoint.local_addr[1])
        a = CollabClient(addr, str(tmp_path / "dl_a"))
        b = CollabClient(addr, str(tmp_path / "dl_b"))
        await a.start(); await b.start()
        await a.login("alice", "alice123"); await b.login("bob", "bob123")
        src = tmp_path / "data.bin"
        payload = bytes(range(256)) * 1500                    # ~384 KB
        src.write_bytes(payload)
        file_id = a.send_file(str(src), "bob")
        await _wait(lambda: file_id in b._incoming_offers)
        b.accept_file(file_id)
        await _wait(lambda: any(e["event"] == "file_done" for e in list(b.events._queue)), timeout=15)
        assert (tmp_path / "dl_b" / "data.bin").read_bytes() == payload
        await a.stop(); await b.stop(); await server.endpoint.stop()
    asyncio.run(run())


# ------------------------------------------------------------------ TODO(B)
@pytest.mark.skip(reason="TODO(B): client rớt đột ngột -> USER_LEFT được gửi cho người còn lại")
def test_client_crash_is_cleaned_up():
    """Gợi ý: đóng transport của client mà không gửi LOGOUT, chờ PEER_TIMEOUT, kiểm registry & USER_LEFT.
    Để test nhanh, tạm giảm constants.PEER_TIMEOUT bằng monkeypatch."""
