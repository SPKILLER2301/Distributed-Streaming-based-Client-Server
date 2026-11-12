"""Transport integration: loss, independent streams, disconnect and cleanup (A7)."""
import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, asynccontextmanager

import pytest

from common import constants as C
from tests.lossy_socket import LossyLink
from transport.crypto import CryptoError, KeyExchange
from transport.endpoint import Endpoint
from transport.flow import ConnectionClosed, FlowLimits


async def _stop_endpoint(endpoint):
    # Detach after cancelling deliveries so CLOSE cannot schedule a new one.
    link, endpoint.link = endpoint.link, None
    handles = ()
    if link is not None:
        handles = tuple(link._handles)
        link.close()
    tick = endpoint._tick_task
    consumers = tuple(endpoint._consumer_tasks)
    await asyncio.wait_for(endpoint.stop(), 2)
    assert all(handle.cancelled() for handle in handles)
    assert link is None or not link._handles
    assert tick is None or tick.done()
    assert all(task.done() for task in consumers)
    assert endpoint._transport is None and endpoint._tick_task is None
    assert not endpoint.connections and not endpoint._hello_by_addr
    assert not endpoint._consumer_tasks and not endpoint._consumer_queues
    assert not endpoint._consumers and endpoint._pending is None


async def _cancel_task(task):
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_crypto_roundtrip_and_tamper():
    a, b = KeyExchange(), KeyExchange()
    ca = a.derive_cipher(b.public_bytes, is_client=True)
    cb = b.derive_cipher(a.public_bytes, is_client=False)
    ct = ca.encrypt(1, 5, b"hdr", b"secret")
    assert cb.decrypt(1, 5, b"hdr", ct) == b"secret"
    with pytest.raises(CryptoError):
        cb.decrypt(1, 5, b"hdr-sua", ct)                      # sửa header -> phát hiện
    with pytest.raises(CryptoError):
        cb.decrypt(1, 6, b"hdr", ct)                          # sai seq/nonce


@asynccontextmanager
async def _pair(link_factory=None, *, algorithm="cubic"):
    """Dựng server + client trên localhost. Trả (server, client, conn_client, conn_server, nhận_ở_server, nhận_ở_client)."""
    got_s, got_c = [], []
    conns = {}
    server = Endpoint(is_server=True, congestion_algorithm=algorithm)
    server.set_handlers(on_connect=lambda cid, addr: conns.setdefault("s", cid),
                        on_receive=lambda cid, sid, d: got_s.append((sid, d)))
    client = Endpoint(is_server=False, congestion_algorithm=algorithm)
    client.set_handlers(on_receive=lambda cid, sid, d: got_c.append((sid, d)))
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_stop_endpoint, server)
        cleanup.push_async_callback(_stop_endpoint, client)
        await server.start(("127.0.0.1", 0))
        await client.start(("127.0.0.1", 0))
        if link_factory:
            server.link, client.link = link_factory(), link_factory()
        cid = await client.connect(("127.0.0.1", server.local_addr[1]))
        yield server, client, cid, conns["s"], got_s, got_c


async def _wait(cond, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while not cond():
        assert asyncio.get_running_loop().time() < end, "hết thời gian chờ"
        await asyncio.sleep(0.01)


def test_handshake_and_messages_both_directions():
    async def run():
        async with _pair() as (server, client, cid, sid_conn, got_s, got_c):
            client.send(cid, C.STREAM_CHAT, b"hello")
            server.send(sid_conn, C.STREAM_CONTROL, b"world")
            await _wait(lambda: got_s and got_c)
            assert got_s == [(C.STREAM_CHAT, b"hello")] and got_c == [(C.STREAM_CONTROL, b"world")]
    asyncio.run(run())


def test_large_message_fragmentation_and_window():
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            blob = bytes(range(256)) * 2000  # ~512 KB, vượt cwnd khởi đầu
            for offset in range(0, len(blob), 8192):
                await client.send_message(cid, 5, blob[offset:offset + 8192])
            await _wait(lambda: sum(len(data) for _, data in got_s) == len(blob), timeout=10)
            assert [sid for sid, _ in got_s] == [5] * ((len(blob) + 8191) // 8192)
            assert b"".join(data for _, data in got_s) == blob
    asyncio.run(run())


def test_messages_keep_order_within_stream():
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            for i in range(200):
                client.send(cid, C.STREAM_CHAT, str(i).encode())
            await _wait(lambda: len(got_s) == 200)
            assert [d for _, d in got_s] == [str(i).encode() for i in range(200)]
    asyncio.run(run())


# ------------------------------------------------------------------ loss / delay / jitter / duplicate
@pytest.mark.parametrize("profile", [
    dict(loss=0.05, delay=0.005, jitter=0.005, duplicate=0.02, seed=5),
    dict(loss=0.10, delay=0.0, jitter=0.0, duplicate=0.0, seed=1),
], ids=["loss5-delay-jitter-duplicate", "loss10"])
def test_delivery_with_loss(profile):
    async def run():
        async with _pair(lambda: LossyLink(**profile)) as (server, client, cid, _, got_s, _):
            # Unique chunk content exposes missing, repeated or reordered messages.
            blob = b"".join(i.to_bytes(4, "big") for i in range(50_000))
            chunks = [blob[offset:offset + 8192] for offset in range(0, len(blob), 8192)]
            peaks = dict(sender_payload=0, receiver_payload=0, cwnd_packets=0)

            async def sample():
                while True:
                    tx, rx = client.get_stats(cid), server.get_stats(cid)
                    used = tx.pending_bytes + tx.unacked_bytes
                    peaks["sender_payload"] = max(peaks["sender_payload"], used)
                    peaks["receiver_payload"] = max(peaks["receiver_payload"], rx.recv_buffered_bytes)
                    peaks["cwnd_packets"] = max(peaks["cwnd_packets"], tx.cwnd_packets)
                    assert used <= client.connections[cid].limits.connection_bytes
                    assert rx.recv_buffered_bytes <= server.connections[cid].limits.connection_bytes
                    assert all(s.pending_bytes + s.unacked_bytes <= client.connections[cid].limits.stream_bytes
                               for s in tx.streams)
                    assert all(s.recv_buffered_bytes <= server.connections[cid].limits.stream_bytes
                               for s in rx.streams)
                    await asyncio.sleep(0.005)

            async with AsyncExitStack() as cleanup:
                monitor = asyncio.create_task(sample())
                cleanup.push_async_callback(_cancel_task, monitor)
                receipts = []
                for chunk in chunks:
                    receipts.append(await client.send_message(cid, 5, chunk))
                await asyncio.wait_for(asyncio.gather(*(r.wait_acked() for r in receipts)), 20)
                # Propagate sampler assertions, rather than hiding them in cleanup.
                if monitor.done():
                    await monitor
            await _wait(lambda: sum(len(data) for _, data in got_s) == len(blob), timeout=20)
            await _wait(lambda: not client.link._handles and not server.link._handles)
            conn = client.connections[cid]
            assert got_s == [(5, chunk) for chunk in chunks]
            assert conn.stats["acked_payload_bytes"] == len(blob)
            assert not conn.send_streams[5].unacked and not conn.send_streams[5].pending
            assert server.link.dropped + client.link.dropped > 0
            print(json.dumps({"case": "loss", "profile_each_direction": profile,
                              "payload_bytes": len(blob), "sha256": hashlib.sha256(blob).hexdigest(),
                              "ack_deadline_seconds": 20, "sampled_peaks": peaks,
                              "client_link": {"sent": client.link.sent, "dropped": client.link.dropped},
                              "server_link": {"sent": server.link.sent, "dropped": server.link.dropped},
                              "stats": conn.stats, "cwnd_packets": conn.cc.cwnd,
                              "pending_packets": len(conn.send_streams[5].pending)}))
    asyncio.run(run())


def test_reorder_and_duplicate():
    async def run():
        async with _pair(lambda: LossyLink(jitter=0.03, duplicate=0.1, seed=2)) as (
                server, client, cid, _, got_s, _):
            for i in range(100):
                client.send(cid, C.STREAM_CHAT, str(i).encode())
            await _wait(lambda: len(got_s) == 100, timeout=10)
            await _wait(lambda: not client.connections[cid].send_streams[1].unacked, timeout=10)
            await _wait(lambda: not client.link._handles and not server.link._handles)
            assert [d for _, d in got_s] == [str(i).encode() for i in range(100)]
            print(json.dumps({"case": "reorder_duplicate", "seed_each_direction": 2,
                              "jitter_seconds_each_direction": 0.03,
                              "duplicate_each_direction": 0.1, "messages": 100,
                              "delivery_deadline_seconds": 10,
                              "stats": client.connections[cid].stats}))
    asyncio.run(run())


@pytest.mark.parametrize("case", ["data", "ack", "tail", "retransmit"])
def test_targeted_loss_over_real_udp(case):
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            client.link = LossyLink(record=True, drop_rule=lambda pkt, n:
                pkt.ptype == C.PKT_DATA and (
                    (case == "data" and pkt.seq == 1 and n == 1) or
                    (case == "tail" and pkt.seq == 7 and n == 1) or
                    (case == "retransmit" and pkt.seq == 1 and n <= 2)))
            server.link = LossyLink(record=True, drop_rule=lambda pkt, n:
                case == "ack" and pkt.ptype == C.PKT_ACK and n == 1)
            expected = [str(i).encode() for i in range(8)]
            for payload in expected:
                assert client.send(cid, 5, payload)
            conn = client.connections[cid]
            await _wait(lambda: len(got_s) == len(expected), timeout=5)
            await _wait(lambda: not conn.send_streams[5].unacked, timeout=5)
            assert got_s == [(5, payload) for payload in expected]
            assert conn.stats["acked_payload_bytes"] == sum(map(len, expected))
            if case in ("data", "retransmit"):
                assert conn.stats["fast_retransmitted"] == 1
            if case in ("ack", "tail", "retransmit"):
                assert conn.stats["timeout_retransmitted"] >= 1
            packets = [event for event in client.link.events
                       if event["packet"].ptype == C.PKT_DATA]
            for seq in range(8):
                assert len({event["wire"] for event in packets
                            if event["packet"].seq == seq}) == 1
            print(json.dumps({"case": "targeted_" + case, "stats": conn.stats}))
    asyncio.run(run())


@pytest.mark.parametrize("algorithm", ["cubic", "aimd"])
def test_no_head_of_line_blocking(algorithm):
    """Chat must arrive while file seq=0 is still withheld, including retries."""
    async def run():
        async with _pair(algorithm=algorithm) as (server, client, cid, _, got_s, _):
            release = asyncio.Event()
            client.link = LossyLink(record=True, drop_rule=lambda pkt, attempt:
                pkt.ptype == C.PKT_DATA and pkt.stream_id == 5 and pkt.seq == 0
                and not release.is_set())
            chunks = [bytes([i]) * 8192 for i in range(4)]
            receipts = [await client.send_message(cid, 5, chunk) for chunk in chunks]
            # Observe the original drop AND an attempted recovery before sending chat.
            await _wait(lambda: client.link.dropped >= 2)
            assert not got_s
            start = asyncio.get_running_loop().time()
            chat_receipt = await client.send_message(cid, C.STREAM_CHAT, b"chat during file gap")
            await _wait(lambda: (C.STREAM_CHAT, b"chat during file gap") in got_s, timeout=2)
            chat_seconds = asyncio.get_running_loop().time() - start
            assert not release.is_set()
            assert got_s == [(C.STREAM_CHAT, b"chat during file gap")]
            assert 0 in client.connections[cid].send_streams[5].unacked
            await asyncio.wait_for(chat_receipt.wait_acked(), 2)

            release.set()
            await asyncio.wait_for(asyncio.gather(*(r.wait_acked() for r in receipts)), 5)
            await _wait(lambda: len(got_s) == len(chunks) + 1)
            assert got_s == [(C.STREAM_CHAT, b"chat during file gap")] + [(5, chunk) for chunk in chunks]
            attempts = [e for e in client.link.events if e["packet"].ptype == C.PKT_DATA
                        and e["packet"].stream_id == 5 and e["packet"].seq == 0]
            assert sum(e["dropped"] for e in attempts) >= 2
            assert any(not e["dropped"] for e in attempts)
            assert len({e["wire"] for e in attempts}) == 1
            stats = client.get_stats(cid)
            assert stats.pending_bytes == stats.unacked_bytes == stats.wire_cache_bytes == 0
            print(json.dumps({"case": "hol", "algorithm": algorithm, "blocked_stream": 5,
                              "blocked_seq": 0, "drops": sum(e["dropped"] for e in attempts),
                              "chat_seconds_while_file_blocked": chat_seconds,
                              "chat_deadline_seconds": 2, "file_bytes": sum(map(len, chunks)),
                              "stats": client.connections[cid].stats}))
    asyncio.run(run())


@pytest.mark.parametrize("cause", ["retries", "timeout"])
def test_peer_disconnect_detected(cause, monkeypatch):
    """Exercise Endpoint's real timer, waiter failures and reclaimed server capacity."""
    monkeypatch.setattr(C, "HEARTBEAT_INTERVAL", 0.05)
    monkeypatch.setattr(C, "PEER_TIMEOUT", 5.0 if cause == "retries" else 0.5)
    monkeypatch.setattr(C, "MAX_RETRIES", 2 if cause == "retries" else 100)
    monkeypatch.setattr(C, "INITIAL_RTO", 0.05 if cause == "retries" else 5.0)
    monkeypatch.setattr(C, "MIN_RTO", 0.02 if cause == "retries" else 5.0)
    monkeypatch.setattr(C, "MAX_RTO", 0.1 if cause == "retries" else 5.0)

    async def run():
        limits = FlowLimits(stream_bytes=32, connection_bytes=64, reserve_bytes=16,
                            control_message=16, file_message=16,
                            stream_messages=8, connection_messages=16,
                            stream_packets=8, connection_packets=16, reserve_packets=4)
        server, client, newcomer = Endpoint(True, flow_limits=limits, max_connections=1), Endpoint(flow_limits=limits), Endpoint(flow_limits=limits)
        closed, received = [], []
        disconnected, waiter_started = asyncio.Event(), asyncio.Event()

        def on_disconnect(cid, reason):
            closed.append((cid, reason))
            disconnected.set()

        server.set_handlers(on_disconnect=on_disconnect,
                            on_receive=lambda cid, sid, data: received.append((cid, data)))
        async with AsyncExitStack() as cleanup:
            for ep in (server, client, newcomer):
                cleanup.push_async_callback(_stop_endpoint, ep)
                await ep.start(("127.0.0.1", 0))
            cid = await client.connect(server.local_addr)
            conn = server.connections[cid]
            original_last_recv = conn.last_recv
            server.link = LossyLink(record=True, drop_rule=lambda pkt, attempt:
                pkt.conn_id == cid and (cause == "timeout" or pkt.ptype == C.PKT_DATA))
            if cause == "timeout":
                client.link = LossyLink(drop_rule=lambda pkt, attempt: pkt.conn_id == cid)
            receipts = [await server.send_message(cid, 5, b"f" * 16) for _ in range(2)]
            ack_tasks = [asyncio.create_task(r.wait_acked()) for r in receipts]

            async def blocked_send():
                waiter_started.set()
                return await server.send_message(cid, 5, b"blocked")

            blocked = asyncio.create_task(blocked_send())
            for task in [*ack_tasks, blocked]:
                cleanup.push_async_callback(_cancel_task, task)
            await asyncio.wait_for(waiter_started.wait(), 1)
            assert not blocked.done() and all(not task.done() for task in ack_tasks)
            stream = conn.send_streams[5]
            assert stream.unacked
            start = asyncio.get_running_loop().time()
            await asyncio.wait_for(disconnected.wait(), 3)
            elapsed = asyncio.get_running_loop().time() - start
            reason = "too many retries" if cause == "retries" else "timeout"
            assert closed == [(cid, reason)]
            for task in [*ack_tasks, blocked]:
                with pytest.raises(ConnectionClosed, match=reason):
                    await asyncio.wait_for(task, 1)
            assert cid not in server.connections
            assert client.local_addr not in server._hello_by_addr
            assert not stream.pending and not stream.unacked and not stream.receipts
            assert not conn.send_streams and not conn.recv_streams
            assert not conn._retransmit_pending and not conn._control_pending
            assert not conn._deliveries and not conn._reset_out and not conn._probes
            stats = conn.get_stats()
            assert stats.state == "closed" and stats.reason == reason
            assert stats.pending_bytes == stats.unacked_bytes == stats.wire_cache_bytes == 0
            assert stats.recv_buffered_bytes == stats.recv_buffered_packets == stats.queued_messages == 0
            if cause == "retries":
                assert conn.last_recv > original_last_recv  # authenticated heartbeat still works
                assert conn.stats["timeout_retransmitted"] >= C.MAX_RETRIES
                attempts = [e["attempt"] for e in server.link.events
                            if e["packet"].ptype == C.PKT_DATA and e["packet"].seq == 0]
                assert max(attempts) == C.MAX_RETRIES + 1
            else:
                assert conn.stats["retransmitted"] == 0  # peer timeout, not retry exhaustion
                assert conn.last_recv == original_last_recv
            conn.on_tick(conn.last_recv + 100)
            conn.close()
            assert closed == [(cid, reason)]

            # Capacity was one: a new handshake and ACK prove the server survived.
            new_id = await asyncio.wait_for(newcomer.connect(server.local_addr), 2)
            receipt = await newcomer.send_message(new_id, C.STREAM_CHAT, b"server alive")
            await asyncio.wait_for(receipt.wait_acked(), 1)
            await _wait(lambda: (new_id, b"server alive") in received, timeout=1)
            assert len(server.connections) == len(server._hello_by_addr) == 1
            assert closed == [(cid, reason)]
            print(json.dumps({"case": "disconnect", "cause": cause, "reason": reason,
                              "elapsed_seconds": elapsed, "deadline_seconds": 3,
                              "max_retries": C.MAX_RETRIES, "peer_timeout_seconds": C.PEER_TIMEOUT,
                              "initial_rto_seconds": C.INITIAL_RTO, "stats": conn.stats,
                              "waiters_failed": 3, "new_peer_acked": True}))
    asyncio.run(run())


def test_cleanup_cancels_delayed_packets_when_assertion_fails():
    """Failure in a scenario must still close sockets/tasks and cancel delayed I/O."""
    async def run():
        with pytest.raises(AssertionError, match="injected scenario failure"):
            async with _pair() as (server, client, cid, _, got_s, _):
                link = client.link = LossyLink(delay=60, duplicate=1.0, seed=7)
                conn = client.connections[cid]
                receipt = await client.send_message(cid, 5, b"never delivered")
                handles = tuple(link._handles)
                assert len(handles) == 2 and not got_s
                raise AssertionError("injected scenario failure")
        assert all(handle.cancelled() for handle in handles)
        assert not link._handles and not got_s
        assert conn.closed and not conn.send_streams
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(receipt.wait_acked(), 1)
        assert server._transport is None and client._transport is None
    asyncio.run(run())
