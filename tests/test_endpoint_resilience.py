"""A6: bounded handshake state, socket failures and deterministic lifecycle."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from common import constants as C
from common.packet import Packet, decode, encode
from transport.crypto import KeyExchange
from transport.endpoint import Endpoint
from transport.flow import ConnectionClosed


PEER = ("127.0.0.1", 9101)


class Datagram:
    """A socket substitute: record sends, inject OSError, signal actual close."""
    def __init__(self, endpoint):
        self.endpoint, self.wire = endpoint, []
        self.error = None
        self.closed = False
        self.stall_close = False
        self.aborted = False

    def sendto(self, raw, addr):
        if self.error is not None:
            raise self.error
        self.wire.append((raw, addr))

    def close(self):
        if not self.closed:
            self.closed = True
            if not self.stall_close:
                asyncio.get_running_loop().call_soon(self.endpoint.connection_lost, None)

    def abort(self):
        self.aborted = True
        asyncio.get_running_loop().call_soon(self.endpoint.connection_lost, None)

    def get_extra_info(self, name):
        return ("127.0.0.1", 9100) if name == "sockname" else None


@asynccontextmanager
async def fake_endpoint(server=True, **kwargs):
    ep = Endpoint(server, **kwargs)
    ep.connection_made(Datagram(ep))
    ep._running = True
    try:
        yield ep
    finally:
        await ep.stop()


def hello(ep, key=None, addr=PEER):
    key = key or KeyExchange()
    ep.datagram_received(encode(Packet(C.PKT_HELLO, payload=key.public_bytes)), addr)
    return key


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "64", 65536])
def test_invalid_connection_cap_is_rejected(value):
    with pytest.raises(ValueError, match="max_connections"):
        Endpoint(max_connections=value)
    assert Endpoint().max_connections == 64


@pytest.mark.parametrize("changes", [
    {"flags": 1}, {"conn_id": 1}, {"stream_id": 1}, {"seq": 1}, {"ack": 1}, {"sack": 1},
    {"payload": b""}, {"payload": b"x" * 31}, {"payload": b"x" * 33},
    {"payload": bytes(32)}, {"payload": b"\x01" + bytes(31)},
])
def test_bad_hello_cannot_replace_existing_session_and_valid_data_still_arrives(changes):
    async def run():
        async with fake_endpoint(max_connections=1) as ep:
            received, disconnected = [], []
            ep.set_handlers(on_receive=lambda cid, sid, data: received.append(data),
                            on_disconnect=lambda *args: disconnected.append(args))
            client = hello(ep)
            conn = next(iter(ep.connections.values()))
            reply = decode(ep._transport.wire[-1][0])
            before = conn.last_recv
            bad = replace(Packet(C.PKT_HELLO, payload=KeyExchange().public_bytes), **changes)
            ep.datagram_received(encode(bad), PEER)
            assert ep.connections == {conn.conn_id: conn}
            assert not conn.closed and not disconnected and conn.last_recv == before
            assert len(ep._hello_by_addr) == 1
            cipher = client.derive_cipher(reply.payload, True)
            pkt = Packet(C.PKT_DATA, conn_id=conn.conn_id, stream_id=1, flags=C.FLAG_EOM)
            aad = pkt.header_bytes(7 + C.GCM_TAG_SIZE)
            ep.datagram_received(aad + cipher.encrypt(1, 0, aad, b"healthy"), PEER)
            assert received == [b"healthy"]
    asyncio.run(run())


def test_capacity_preserves_live_sessions_and_duplicate_hello_uses_cached_reply():
    async def run():
        async with fake_endpoint(max_connections=1) as ep:
            client = hello(ep)
            conn = next(iter(ep.connections.values()))
            reply = ep._transport.wire[-1][0]
            for port in range(9200, 9220):
                hello(ep, addr=("127.0.0.1", port))
            assert len(ep.connections) == len(ep._hello_by_addr) == 1
            assert ep.stats["resource_rejections"] == 20
            hello(ep, client)
            assert ep._transport.wire[-1][0] == reply
            assert next(iter(ep.connections.values())) is conn
            ep.close(conn.conn_id)
            assert not ep.connections and not ep._hello_by_addr
            hello(ep, addr=("127.0.0.1", 9200))
            assert len(ep.connections) == 1
    asyncio.run(run())


def test_valid_restart_at_capacity_replaces_once_with_fresh_identity():
    async def run():
        async with fake_endpoint(max_connections=1) as ep:
            closed = []
            ep.set_handlers(on_disconnect=lambda cid, reason: closed.append((cid, reason)))
            hello(ep)
            old = next(iter(ep.connections.values()))
            hello(ep)
            new = next(iter(ep.connections.values()))
            assert old.closed and new.conn_id != old.conn_id
            assert closed == [(old.conn_id, "peer_restarted")]
            assert len(ep._hello_by_addr) == 1
    asyncio.run(run())


def test_connection_id_collision_is_bounded_without_evicting_old_peer(monkeypatch):
    async def run():
        async with fake_endpoint() as ep:
            hello(ep)
            old = next(iter(ep.connections.values()))
            calls = []

            def collision(size):
                calls.append(size)
                return old.conn_id.to_bytes(4, "big")

            monkeypatch.setattr("transport.endpoint.os.urandom", collision)
            hello(ep)
            assert len(calls) == 16
            assert ep.connections == {old.conn_id: old} and not old.closed
    asyncio.run(run())


@pytest.mark.parametrize("changes", [{"conn_id": 0}, {"flags": 1}, {"stream_id": 1},
    {"seq": 1}, {"ack": 1}, {"sack": 1}, {"payload": bytes(32)}, {"payload": b"x" * 31}])
def test_invalid_reply_does_not_finish_pending_handshake(changes):
    async def run():
        async with fake_endpoint(server=False) as ep:
            task = asyncio.create_task(ep.connect(PEER))
            try:
                await asyncio.sleep(0)
                assert ep._pending is not None
                valid = Packet(C.PKT_HELLO_REPLY, conn_id=7, payload=KeyExchange().public_bytes)
                ep.datagram_received(encode(replace(valid, **changes)), PEER)
                assert not task.done() and not ep.connections
                ep.datagram_received(encode(valid), ("127.0.0.1", 9999))
                assert not task.done()
                ep.datagram_received(encode(valid), PEER)
                assert await asyncio.wait_for(task, 1) == 7
                conn = ep.connections[7]
                ep.datagram_received(encode(valid), PEER)
                assert ep.connections[7] is conn
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_reply_cannot_overwrite_existing_connection_id():
    async def run():
        async with fake_endpoint(server=False) as ep:
            a, b = KeyExchange(), KeyExchange()
            old = ep._make_connection(7, PEER, a.derive_cipher(b.public_bytes, True))
            task = asyncio.create_task(ep.connect(PEER))
            try:
                await asyncio.sleep(0)
                reply = Packet(C.PKT_HELLO_REPLY, conn_id=7, payload=b.public_bytes)
                ep.datagram_received(encode(reply), PEER)
                assert not task.done() and ep.connections[7] is old and not old.closed
                ep.datagram_received(encode(replace(reply, conn_id=8)), PEER)
                assert await asyncio.wait_for(task, 1) == 8
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_socket_errors_are_nonfatal_and_logs_are_rate_limited(caplog):
    async def run():
        async with fake_endpoint() as ep:
            hello(ep)
            conn = next(iter(ep.connections.values()))
            ep._transport.error = ConnectionResetError(10054, "simulated UDP reset")
            for _ in range(10):
                ep.sendto(b"test", PEER)
                ep.error_received(ConnectionResetError(10054, "ICMP"))
            assert ep.stats["socket_errors"] == 20
            assert not conn.closed
            ep._transport.error = None
            conn.send_message(1, b"still sending")
            assert decode(ep._transport.wire[-1][0]).ptype == C.PKT_DATA
    asyncio.run(run())
    records = [r for r in caplog.records if r.name == "transport.endpoint"]
    assert len(records) == 1


def test_peer_timeout_frees_capacity_and_handshake_cache():
    async def run():
        async with fake_endpoint(max_connections=1) as ep:
            hello(ep)
            conn = next(iter(ep.connections.values()))
            conn.on_tick(conn.last_recv + C.PEER_TIMEOUT + 0.1)
            assert conn.closed and not ep.connections and not ep._hello_by_addr
            hello(ep, addr=("127.0.0.1", 9300))
            assert len(ep.connections) == 1
    asyncio.run(run())


def test_unexpected_socket_loss_wakes_receipt_and_cleans_connection_state():
    async def run():
        async with fake_endpoint() as ep:
            hello(ep)
            conn = next(iter(ep.connections.values()))
            receipt = conn.send_message(1, b"unacked")
            waiter = asyncio.create_task(receipt.wait_acked())
            transport = ep._transport
            transport.close()
            with pytest.raises(ConnectionClosed, match="socket_closed"):
                await asyncio.wait_for(waiter, 1)
            assert not ep.connections and not ep._hello_by_addr
            assert conn.get_stats().wire_cache_bytes == 0
    asyncio.run(run())


def test_stop_with_exhausted_control_counter_still_closes_every_connection():
    async def run():
        async with fake_endpoint() as ep:
            hello(ep)
            hello(ep, addr=("127.0.0.1", 9200))
            connections = list(ep.connections.values())
            connections[0]._control_counter = 1 << 64
            await asyncio.wait_for(ep.stop(), 1)
            assert all(conn.closed for conn in connections)
            assert not ep.connections and ep._transport is None
    asyncio.run(run())


def test_stop_aborts_stalled_udp_close_and_still_waits_for_socket_callback():
    async def run():
        async with fake_endpoint() as ep:
            hello(ep)
            transport = ep._transport
            transport.stall_close = True
            conn = next(iter(ep.connections.values()))
            receipt = conn.send_message(1, b"pending")
            await asyncio.wait_for(ep.stop(), 1)
            assert transport.aborted and ep._socket_closed.done() and ep._transport is None
            assert isinstance(receipt._error, ConnectionClosed)
            assert not ep.connections and not ep._hello_by_addr
    asyncio.run(run())


def test_lifecycle_rejects_double_start_and_supports_stop_restart():
    async def run():
        ep = Endpoint()
        with pytest.raises(ConnectionClosed):
            await ep.connect(PEER)
        with pytest.raises(RuntimeError):
            _ = ep.local_addr
        try:
            await ep.start(("127.0.0.1", 0))
            tick = ep._tick_task
            with pytest.raises(RuntimeError):
                await ep.start(("127.0.0.1", 0))
            assert ep._tick_task is tick
            await asyncio.wait_for(asyncio.gather(ep.stop(), ep.stop()), 1)
            assert tick.done() and ep._tick_task is None and ep._transport is None
            await ep.start(("127.0.0.1", 0))
            assert ep._tick_task is not tick
        finally:
            await ep.stop()
    asyncio.run(run())


def test_socket_loss_wakes_pending_handshake_and_late_reply_cannot_recreate_it():
    async def run():
        async with fake_endpoint(server=False) as ep:
            transport = ep._transport
            task = asyncio.create_task(ep.connect(PEER))
            await asyncio.sleep(0)
            transport.close()
            with pytest.raises(ConnectionClosed, match="socket_closed"):
                await asyncio.wait_for(task, 1)
            ep.datagram_received(encode(Packet(C.PKT_HELLO_REPLY, conn_id=7,
                                               payload=KeyExchange().public_bytes)), PEER)
            assert not ep.connections and ep._pending is None
    asyncio.run(run())


def test_stop_cancels_handler_that_calls_stop_in_its_finally_without_deadlock():
    async def run():
        ep = Endpoint(True)
        client = Endpoint()
        started, cleaned = asyncio.Event(), asyncio.Event()

        async def handler(message):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await ep.stop()
                cleaned.set()

        ep.set_message_handler(handler)
        try:
            await ep.start(("127.0.0.1", 0))
            await client.start(("127.0.0.1", 0))
            cid = await client.connect(ep.local_addr)
            await client.send_message(cid, 1, b"block")
            await asyncio.wait_for(started.wait(), 1)
            await asyncio.wait_for(ep.stop(), 1)
            assert cleaned.is_set() and not ep._consumer_tasks and ep._transport is None
        finally:
            await client.stop()
            await ep.stop()
    asyncio.run(run())


def test_failed_bind_can_be_retried_and_start_stop_are_serialized():
    async def run():
        owner, ep = Endpoint(), Endpoint()
        try:
            await owner.start(("127.0.0.1", 0))
            with pytest.raises(OSError):
                await ep.start(owner.local_addr)
            assert ep._transport is None and ep._tick_task is None
            await asyncio.wait_for(asyncio.gather(ep.start(("127.0.0.1", 0)), ep.stop()), 1)
            assert ep._transport is None and ep._tick_task is None
        finally:
            await ep.stop()
            await owner.stop()
    asyncio.run(run())


def test_client_cancellation_releases_pending_handshake_and_can_retry():
    async def run():
        async with fake_endpoint(server=False) as ep:
            task = asyncio.create_task(ep.connect(PEER))
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert ep._pending is None and not ep.connections
            retry = asyncio.create_task(ep.connect(PEER))
            await asyncio.sleep(0)
            ep.datagram_received(encode(Packet(C.PKT_HELLO_REPLY, conn_id=7,
                                               payload=KeyExchange().public_bytes)), PEER)
            assert await asyncio.wait_for(retry, 1) == 7
    asyncio.run(run())


def test_real_udp_bad_input_and_capacity_do_not_break_established_chat(monkeypatch):
    async def run():
        server, client, extra = Endpoint(True, max_connections=1), Endpoint(), Endpoint()
        received = asyncio.Queue()
        server.set_handlers(on_receive=lambda cid, sid, data: received.put_nowait(data))
        try:
            await server.start(("127.0.0.1", 0))
            await client.start(("127.0.0.1", 0))
            await extra.start(("127.0.0.1", 0))
            cid = await client.connect(server.local_addr)
            client.sendto(b"garbage", server.local_addr)
            client.sendto(encode(Packet(C.PKT_HELLO, payload=bytes(32))), server.local_addr)
            monkeypatch.setattr(C, "HANDSHAKE_TIMEOUT", 0.05)
            monkeypatch.setattr(C, "HANDSHAKE_RETRIES", 1)
            with pytest.raises(ConnectionError):
                await extra.connect(server.local_addr)
            receipt = await client.send_message(cid, 1, b"healthy chat")
            assert await asyncio.wait_for(received.get(), 1) == b"healthy chat"
            await asyncio.wait_for(receipt.wait_acked(), 1)
            assert len(server.connections) == len(server._hello_by_addr) == 1
            assert server.stats["invalid_packets"] >= 1
            assert server.stats["invalid_handshakes"] >= 1
            assert server.stats["resource_rejections"] >= 1
        finally:
            await extra.stop()
            await client.stop()
            await server.stop()
    asyncio.run(run())
