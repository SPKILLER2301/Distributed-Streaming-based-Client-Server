import asyncio
import hashlib
from pathlib import Path
import pytest
from common import messages as M
from server.auth import Auth, hash_password
from server.registry import Registry
from server.router import Router
from client.file_transfer import FileReceiver
from tests.app_helpers import session, event, wait_until, USERS
from tests.load_test import run_load


@pytest.mark.parametrize("size", [0, 1, 8192, 8193])
def test_file_boundaries_over_relay(tmp_path, size):
    async def run():
        async with session(tmp_path) as (server, (a, b)):
            source = tmp_path / "Tiếng Việt.txt"
            payload = bytes((i % 251 for i in range(size)))
            source.write_bytes(payload)
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            b.accept_file(fid)
            assert (await event(b, "file_done"))["ok"]
            assert (await event(a, "file_done"))["ok"]
            assert (tmp_path / "1" / source.name).read_bytes() == payload
            assert not server.router.transfers and not b._receivers and not a._outgoing

    asyncio.run(run())


def test_auth_duplicate_repeat_and_connection_identity(tmp_path):
    async def run():
        async with session(tmp_path, 3) as (server, (a, b, c)):
            assert (await a.login("alice", "alice123"))[0]
            assert not (await a.login("bob", "bob123"))[0]
            assert server.registry.get_by_name("alice").conn_id == a.conn_id
            await c.logout()
            # Duplicate username on another freshly connected client.
            from client.client import CollabClient

            other = CollabClient(server._bootstrap.local_addr)
            try:
                await other.start()
                assert not (await other.login("alice", "alice123"))[0]
            finally:
                await other.stop()
            assert server.registry.usernames() == ["alice", "bob"]

    asyncio.run(run())


def test_private_message_does_not_reach_third_user(tmp_path):
    async def run():
        async with session(tmp_path, 3) as (_, (a, b, c)):
            a.send_chat("secret", "bob")
            assert (await event(b, "chat"))["text"] == "secret"
            with pytest.raises(asyncio.TimeoutError):
                await event(c, "chat", timeout=0.2)

    asyncio.run(run())


def test_reconnect_auth_and_stop(tmp_path):
    async def run():
        async with session(tmp_path) as (server, (a, b)):
            old = a.conn_id
            server.endpoint.close(old)
            await wait_until(
                lambda: a.conn_id not in (None, old) and a.username == "alice", 5
            )
            assert len([u for u in server.registry.all() if u.username == "alice"]) == 1
            await a.logout()
            assert a._credentials is None and a._reconnect_task is None
            assert not a._outgoing and not a._receivers

    asyncio.run(run())


def test_rejected_file_cleanup_and_disconnect_pending_offer(tmp_path):
    async def run():
        async with session(tmp_path) as (server, (a, b)):
            source = tmp_path / "a.bin"
            source.write_bytes(b"test")
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            b.reject_file(fid)
            await wait_until(lambda: fid not in a._outgoing)
            assert fid not in server.router.transfers
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            await a.stop()
            await wait_until(lambda: fid not in b._incoming_offers)
            assert not server.router.transfers

    asyncio.run(run())


def test_receiver_overflow_aborts_file(tmp_path):
    rx = FileReceiver("x", "file", 1, hashlib.sha256(b"a").hexdigest(), str(tmp_path))
    with pytest.raises(ValueError):
        rx.write(b"xx")
    assert not Path(rx.path).exists()


def test_password_salts_are_unique_and_legacy_hashes_rejected(tmp_path):
    import json

    first, second = hash_password("password"), hash_password("password")
    assert first != second
    users = tmp_path / "users.json"
    users.write_text(json.dumps({"a": first, "old": "a" * 64}))
    auth = Auth(users)
    assert auth.verify("a", "password") and not auth.verify("a", "wrong")
    assert not auth.verify("old", "password")


class FakeEndpoint:
    def __init__(self):
        self.sent = []

    def send(self, cid, sid, data):
        self.sent.append((cid, sid, M.decode(data)))
        return True

    def open_stream(self, cid):
        return 2

    async def send_message(self, cid, sid, data):
        return self.send(cid, sid, data)

    async def cancel_stream(self, cid, sid, reason):
        pass


def make_router():
    registry = Registry()
    for i, name in enumerate(("alice", "bob", "carol"), 1):
        registry.add(name, i, ("127.0.0.1", i))
    return Router(FakeEndpoint(), registry)


def offer(router):
    router.forward_signal(
        "alice",
        dict(
            type=M.FILE_OFFER,
            to="bob",
            file_id="x",
            size=1,
            name="f",
            sha256=hashlib.sha256(b"x").hexdigest(),
        ),
    )


def test_file_state_prevents_spoof_accept_begin_and_done():
    async def run():
        router = make_router()
        offer(router)
        with pytest.raises(ValueError):
            router.forward_signal(
                "carol", dict(type=M.FILE_ACCEPT, to="alice", file_id="x")
            )
        begin = M.encode(M.FILE_BEGIN, to="bob", file_id="x", size=1)
        with pytest.raises(ValueError):
            await router.relay_file_stream(1, 2, begin)
        router.forward_signal("bob", dict(type=M.FILE_ACCEPT, to="alice", file_id="x"))
        with pytest.raises(ValueError):
            router.forward_signal("bob", dict(type=M.FILE_ACCEPT, to="alice", file_id="x"))
        with pytest.raises(ValueError):
            router.forward_signal(
                "bob", dict(type=M.FILE_DONE, to="alice", file_id="x", ok=True)
            )
        await router.relay_file_stream(1, 2, begin)
        with pytest.raises(ValueError):
            await router.relay_file_stream(1, 2, b"xx")
        assert not router.transfers and not router._relays

    asyncio.run(run())


@pytest.mark.parametrize("bad", [True, -1, "1", M.MAX_FILE_SIZE + 1])
def test_file_size_schema(bad):
    with pytest.raises(ValueError):
        M.validate_client(dict(type=M.FILE_BEGIN, to="bob", file_id="x", size=bad), 2)


def test_registry_cannot_bind_two_users_to_one_connection():
    r = Registry()
    assert r.add("a", 1, ("x", 1))
    assert not r.add("b", 1, ("x", 1))


def test_load_twenty_clients(tmp_path):
    from server.server import CollabServer

    async def run():
        server = CollabServer("127.0.0.1", 0, USERS)
        await server.start()
        try:
            result = await run_load(server.endpoint.local_addr, 20)
            assert result["received"] == result["expected"] == 380
            assert result["duplicates"] == 0
            print(result)
        finally:
            await server.stop()

    asyncio.run(run())


def test_disconnect_during_file_deletes_partial_and_cancels_producer(tmp_path):
    async def run():
        async with session(tmp_path) as (server, (a, b)):
            source = tmp_path / "partial.bin"
            source.write_bytes(b"a" * 20000)

            async def pause_sender(fid):
                sender, to = a._outgoing[fid]
                sid = a.endpoint.open_stream(a.conn_id)
                await a._send_data(
                    a.conn_id,
                    sid,
                    M.encode(M.FILE_BEGIN, to=to, file_id=fid, size=sender.size),
                )
                await a._send_data(a.conn_id, sid, b"a" * 8192)
                await asyncio.Event().wait()

            a._send_file_data = pause_sender
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            b.accept_file(fid)
            await event(b, "file_progress", lambda ev: ev["done"] == 8192)
            partial = Path(b._receivers[fid].path)
            assert partial.exists()
            await a.stop()
            await wait_until(lambda: fid not in b._receivers)
            assert not partial.exists() and not server.router.transfers
            assert not a._send_tasks

    asyncio.run(run())


def test_acked_progress_adapter_waits_for_receipt():
    from client.client import CollabClient

    async def run():
        c = CollabClient(("127.0.0.1", 1))
        acked = asyncio.Event()

        class Receipt:
            async def wait_acked(self):
                await acked.wait()

        async def send(*args):
            return Receipt()

        c.endpoint.send_message = send
        task = asyncio.create_task(c._send_data(1, 2, b"payload"))
        await asyncio.sleep(0)
        assert not task.done()
        acked.set()
        assert await task == "acked_to_server"

    asyncio.run(run())


def test_enqueue_failure_still_cleans_file_when_error_cannot_be_sent(tmp_path):
    from client.client import CollabClient
    from client.file_transfer import FileSender

    async def run():
        source = tmp_path / "enqueue.bin"
        source.write_bytes(b"x")
        sender = FileSender(str(source))
        c = CollabClient(("127.0.0.1", 1))
        c.conn_id = 1
        c._outgoing[sender.file_id] = (sender, "bob")
        c.endpoint.open_stream = lambda cid: 2
        c.endpoint.send = lambda *args: False
        await c._send_file_data(sender.file_id)
        assert not c._outgoing and not c._send_tasks

    asyncio.run(run())


def test_relay_retains_receive_credit_until_enqueue_and_keeps_chat_live(tmp_path, monkeypatch):
    async def run():
        gate, entered = asyncio.Event(), asyncio.Event()
        async with session(tmp_path) as (server, (a, b)):
            send = server.endpoint.send_message

            async def blocked_enqueue(cid, sid, data):
                if cid == b.conn_id and sid >= 2 and len(data) == 8192:
                    entered.set()
                    await gate.wait()
                return await send(cid, sid, data)

            monkeypatch.setattr(server.endpoint, "send_message", blocked_enqueue)
            source = tmp_path / "backpressure.bin"
            payload = b"a" * 8192 + b"z"
            source.write_bytes(payload)
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            b.accept_file(fid)
            try:
                await asyncio.wait_for(entered.wait(), 2)
                source_conn = server.endpoint.connections[a.conn_id]
                assert source_conn.get_stats().recv_buffered_bytes >= 8192
                assert source_conn._deliveries  # ownership stays with the blocked consumer
                assert server.router.transfers[fid].remaining == len(payload)
                a.send_chat("chat while relay waits", to="bob")
                assert (await event(b, "chat"))["text"] == "chat while relay waits"
                assert not gate.is_set() and fid in a._outgoing
                assert not any(ev["event"] == "file_done" for ev in a.events._queue)
                gate.set()
                received, sent = await asyncio.gather(
                    event(b, "file_done", lambda ev: ev["file_id"] == fid),
                    event(a, "file_done", lambda ev: ev["file_id"] == fid),
                )
                assert received["ok"] and sent["ok"]
                assert (tmp_path / "1" / source.name).read_bytes() == payload
                await wait_until(lambda: source_conn.get_stats().recv_buffered_bytes == 0)
            finally:
                gate.set()

    asyncio.run(run())


def test_first_hop_ack_waits_for_file_done_and_releases_transport_streams(tmp_path, monkeypatch):
    async def run():
        async with session(tmp_path) as (server, (a, b)):
            deferred = []
            forward = server.router.forward_signal

            def hold_done(sender, msg):
                if msg["type"] == M.FILE_DONE:
                    deferred.append((sender, msg))
                else:
                    forward(sender, msg)

            monkeypatch.setattr(server.router, "forward_signal", hold_done)
            source = tmp_path / "ack-is-not-done.bin"
            source.write_bytes(b"verified content")
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers)
            b.accept_file(fid)
            assert (await event(b, "file_done"))["ok"]
            progress = await event(a, "file_progress")
            assert progress["stage"] == "acked_to_server"
            assert progress["done"] == source.stat().st_size
            await wait_until(lambda: bool(deferred) and fid not in a._send_tasks)
            assert fid in a._outgoing
            assert not any(ev["event"] == "file_done" for ev in a.events._queue)
            assert server.router.transfers[fid].state == "await_done"
            cid, sid = a._send_streams[fid]
            _, _, relay_cid, relay_sid = server.router.transfers[fid].relay
            assert not a.endpoint.connections[cid].send_streams[sid].cancelled
            forward(*deferred[0])
            assert (await event(a, "file_done"))["ok"]
            await wait_until(lambda: not a._stream_cleanup_tasks and not server.router._cleanup_tasks)
            assert a.endpoint.connections[cid].send_streams[sid].cancelled
            assert server.endpoint.connections[relay_cid].send_streams[relay_sid].cancelled
            assert not a._send_streams and not server.router.transfers

    asyncio.run(run())
