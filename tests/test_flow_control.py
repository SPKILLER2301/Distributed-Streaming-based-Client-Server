"""A5 credit, bounded memory, fairness and Endpoint handoff tests."""
import asyncio
import hashlib
import json
import tracemalloc
import statistics
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from common import constants as C
from common.packet import Packet, decode
from transport.connection import Connection
from transport.crypto import KeyExchange
from transport.endpoint import Endpoint
from transport.flow import (ConnectionClosed, FlowLimits, MessageTooLarge,
                            QueueFull, StreamCancelled, StreamLimitError)
from transport.flow_control import CREDIT


def small_limits():
    return FlowLimits(stream_bytes=32, connection_bytes=64, reserve_bytes=16,
                      control_message=16, file_message=16,
                      stream_messages=8, connection_messages=16,
                      stream_packets=8, connection_packets=16, reserve_packets=4,
                      active_streams=8, lifetime_streams=32)


class Network:
    """Real encrypted Connections with bounded queues and a fake clock."""
    def __init__(self, limits=None, drop=None, record=True):
        self.now = 100.0
        self.queue = deque()
        self.held = {"a": [], "b": []}
        self.nodes = {}
        self.events = []
        self.drop = drop
        a, b = KeyExchange(), KeyExchange()
        for name, cipher in (("a", a.derive_cipher(b.public_bytes, True)),
                             ("b", b.derive_cipher(a.public_bytes, False))):
            def send(raw, addr, source=name):
                pkt = decode(raw)
                dropped = bool(self.drop and self.drop(source, pkt))
                if record:
                    self.events.append((self.now, source, pkt, raw, dropped))
                if not dropped:
                    self.queue.append(("b" if source == "a" else "a", raw))

            self.nodes[name] = Connection(7, ("peer", 9000), cipher, send,
                lambda *args: None, lambda *args: None, clock=lambda: self.now,
                flow_limits=limits or small_limits(),
                on_delivery=lambda msg, dest=name: self.held[dest].append(msg))

    def flush(self):
        count = 0
        while self.queue:
            dest, raw = self.queue.popleft()
            self.nodes[dest].handle_packet(decode(raw))
            count += 1
            assert count < 10000

    def step(self, seconds=0.021):
        self.now += seconds
        for conn in self.nodes.values():
            conn.on_tick(self.now)
        self.flush()

    def consume(self, side="b", count=None):
        messages = self.held[side][:count]
        del self.held[side][:len(messages)]
        for message in messages:
            self.nodes[side].consume(message)
        self.flush()


@asynccontextmanager
async def endpoints(handler, *, limits=None):
    server, client = Endpoint(True, flow_limits=limits), Endpoint(flow_limits=limits)
    server.set_message_handler(handler(server))
    try:
        await server.start(("127.0.0.1", 0))
        await client.start(("127.0.0.1", 0))
        cid = await client.connect(server.local_addr)
        yield server, client, cid
    finally:
        for ep in (client, server):
            if ep.link is not None:
                ep.link.close()
            await ep.stop()


async def eventually(predicate, timeout=3):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(wait(), timeout)


def test_credit_is_only_returned_on_consume_and_duplicate_is_idempotent():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    a.send_message(2, b"x" * 16)
    a.send_message(2, b"y" * 16)
    network.flush()
    assert b.get_stats().recv_buffered_bytes == 32
    a.send_message(2, b"z" * 16)
    network.flush()
    assert len(network.held["b"]) == 2
    assert a.get_stats().pending_bytes == 16
    message = network.held["b"].pop(0)
    b.consume(message)
    b.consume(message)
    network.step()
    assert [msg.data for msg in network.held["b"]] == [b"y" * 16, b"z" * 16]
    assert b.get_stats().recv_buffered_bytes == 32
    assert a.send_streams[2].peer_credit[0] == 48


def test_lost_credit_and_probe_do_not_deadlock():
    dropped = {C.PKT_CREDIT: False, C.PKT_CREDIT_PROBE: False}

    def drop(source, pkt):
        if pkt.ptype in dropped and not dropped[pkt.ptype]:
            dropped[pkt.ptype] = True
            return True
        return False

    network = Network(drop=drop)
    a = network.nodes["a"]
    for _ in range(2):
        a.send_message(2, b"x" * 16)
    network.flush()
    a.send_message(2, b"last")
    network.consume(count=1)
    for _ in range(150):
        network.step()
        if any(msg.data == b"last" for msg in network.held["b"]):
            break
    assert all(dropped.values())
    assert any(msg.data == b"last" for msg in network.held["b"])


def test_replayed_and_reordered_credit_cannot_inflate_limits():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    a.send_message(2, b"a")
    network.flush()
    network.consume()
    network.step()
    raw = next(raw for _, src, pkt, raw, _ in reversed(network.events)
               if src == "b" and pkt.ptype == C.PKT_CREDIT)
    before = a.send_streams[2].peer_credit
    for _ in range(10):
        a.handle_packet(decode(raw))
    b._transmit(Packet(C.PKT_CREDIT, stream_id=2,
                       payload=CREDIT.pack(32, 64, 48, 8, 16, 12)))
    network.step()
    assert a.send_streams[2].peer_credit == before


def test_receiver_enforces_byte_budget_before_ack_and_retransmit_costs_no_credit():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    for _ in range(2):
        a.send_message(2, b"x" * 16)
    network.flush()
    before = b.get_stats()
    raw = next(raw for _, src, pkt, raw, _ in network.events if src == "a" and pkt.ptype == C.PKT_DATA)
    b.handle_packet(decode(raw))
    assert b.get_stats().recv_buffered_bytes == before.recv_buffered_bytes
    assert b.get_stats().recv_buffered_packets == before.recv_buffered_packets
    # Authenticated but noncompliant sender exceeds the granted receive window.
    b.handle_packet(decode(a._encode_packet(Packet(C.PKT_DATA, stream_id=2, seq=2,
                                                  flags=C.FLAG_EOM, payload=b"extra"))))
    assert b.get_stats().recv_buffered_bytes == 32
    assert b.recv_streams[2].expected == 2
    assert b.stats["flow_blocked"] == 1


def test_empty_messages_are_limited_by_packet_credit():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    for _ in range(8):
        a.send_message(2, b"")
    network.flush()
    network.step()
    a.send_message(2, b"")
    network.flush()
    assert len(network.held["b"]) == 8
    assert b.get_stats().recv_buffered_bytes == 0
    assert b.get_stats().recv_buffered_packets == 8
    network.consume(count=1)
    network.step()
    assert len(network.held["b"]) == 8
    assert b.get_stats().recv_buffered_packets == 8


def test_connection_reserve_preserves_chat_when_files_fill_credit():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    for sid in (2, 3, 4):
        a.send_message(sid, b"f" * 16)
    network.flush()
    a.send_message(5, b"blocked file")
    receipt = a.send_message(1, b"chat")
    network.step()
    assert any(msg.stream_id == 1 and msg.data == b"chat" for msg in network.held["b"])
    assert not any(msg.stream_id == 5 for msg in network.held["b"])
    assert receipt.acked_bytes == 4
    assert b.get_stats().recv_buffered_bytes <= 64


def test_weighted_scheduler_and_file_cursor_persist_across_pumps():
    network = Network(limits=FlowLimits())
    a = network.nodes["a"]
    a.cc.allowed_inflight = lambda: 0
    for _ in range(16):
        a.send_message(1, b"chat")
    for sid in (2, 3):
        for _ in range(4):
            a.send_message(sid, b"file")
    # Free precisely one slot each time, mirroring many separate ACK pumps.
    for _ in range(15):
        # Freeze the budget per pump instead of allowing infinite new slots.
        budget = a._inflight_total() + 1
        a.cc.allowed_inflight = lambda budget=budget: budget
        a._pump()
    sequence = [pkt.stream_id for _, src, pkt, _, _ in network.events
                if src == "a" and pkt.ptype == C.PKT_DATA]
    assert sequence == [1, 1, 1, 1, 2, 1, 1, 1, 1, 3, 1, 1, 1, 1, 2]


def test_close_wakes_enqueue_and_ack_waiters_and_cancellation_before_commit():
    async def run():
        network = Network()
        a = network.nodes["a"]
        receipt = a.send_message(2, b"x" * 16)
        a.send_message(2, b"y" * 16)
        blocked = asyncio.create_task(a.enqueue_message(2, b"z"))
        ack_wait = asyncio.create_task(receipt.wait_acked())
        await asyncio.sleep(0)
        assert not blocked.done()
        blocked.cancel()
        with pytest.raises(asyncio.CancelledError):
            await blocked
        assert a.send_streams[2].next_seq == 2
        blocked = asyncio.create_task(a.enqueue_message(2, b"z"))
        await asyncio.sleep(0)
        a.close()
        for task in (blocked, ack_wait):
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(task, 1)
        assert a.get_stats().queued_messages == 0
    asyncio.run(run())


def test_receipt_counts_new_payload_and_cancelled_waiter_does_not_cancel_data():
    async def run():
        network = Network()
        a = network.nodes["a"]
        receipt = a.send_message(2, b"payload")
        waiter = asyncio.create_task(receipt.wait_acked())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        network.flush()
        await asyncio.wait_for(receipt.wait_acked(), 1)
        assert receipt.payload_bytes == receipt.acked_bytes == 7
    asyncio.run(run())


@pytest.mark.parametrize("lost_type", [C.PKT_RESET_STREAM, C.PKT_RESET_ACK])
def test_reset_retries_preserve_direction_credit_and_other_streams(lost_type):
    async def run():
        dropped = False

        def drop(source, pkt):
            nonlocal dropped
            if pkt.ptype == lost_type and not dropped:
                dropped = True
                return True
            return False

        network = Network(drop=drop)
        a, b = network.nodes["a"], network.nodes["b"]
        receipt = a.send_message(2, b"file")
        opposite = b.send_message(2, b"opposite")
        task = asyncio.create_task(a.cancel_stream(2))
        await asyncio.sleep(0)
        network.flush()
        for _ in range(100):
            network.step()
            if not a._reset_out:
                break
        await asyncio.wait_for(task, 1)
        with pytest.raises(StreamCancelled):
            await receipt.wait_acked()
        await opposite.wait_acked()
        assert any(msg.data == b"opposite" for msg in network.held["a"])
        assert b.get_stats().recv_buffered_bytes == 0
        old_consumed = b._consumed_bytes
        # Late original DATA cannot resurrect a reset stream or return credit twice.
        original = next(raw for _, source, pkt, raw, _ in network.events
                        if source == "a" and pkt.ptype == C.PKT_DATA)
        b.handle_packet(decode(original))
        assert b._consumed_bytes == old_consumed
        a.send_message(1, b"chat")
        network.step()
        assert any(msg.data == b"chat" for msg in network.held["b"])
    asyncio.run(run())


def test_limits_and_forged_consume_token():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    with pytest.raises(MessageTooLarge):
        a.send_message(2, b"x" * 17)
    for sid in range(2, 8):
        a._send_stream(sid)
    with pytest.raises(StreamLimitError):
        a._send_stream(8)
    a.send_message(2, b"x")
    network.flush()
    msg = network.held["b"][0]
    with pytest.raises(ValueError):
        a.consume(msg)
    with pytest.raises(ValueError):
        b.consume(replace(msg))
    b.consume(msg)


def test_missing_eom_cannot_grow_fragments_beyond_message_limit():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    for seq in range(2):
        raw = a._encode_packet(Packet(C.PKT_DATA, stream_id=2, seq=seq, payload=b"x" * 10))
        b.handle_packet(decode(raw))
    assert b.closed and b.reason == "protocol_error"
    assert not b.recv_streams and not b._deliveries


def test_control_tamper_and_replay_do_not_change_credit_or_liveness():
    network = Network()
    a, b = network.nodes["a"], network.nodes["b"]
    a.send_message(2, b"x")
    network.flush()
    raw = b._encode_packet(Packet(C.PKT_CREDIT, stream_id=2,
        payload=CREDIT.pack(100, 200, 180, 30, 60, 50)))
    bad = bytearray(raw)
    bad[-1] ^= 1
    before = a.last_recv
    network.now += 0.1
    a.handle_packet(decode(bytes(bad)))
    assert a.last_recv == before and a._peer_conn_bytes == 64
    a.handle_packet(decode(raw))
    assert a._peer_conn_bytes == 200
    last = a.last_recv
    network.now += 0.1
    a.handle_packet(decode(raw))
    assert a.last_recv == last


def test_slow_async_file_handler_does_not_block_chat_and_stop_cleans_tasks():
    async def run():
        gate, file_started, chat = asyncio.Event(), asyncio.Event(), asyncio.Event()

        def handler(ep):
            async def receive(msg):
                if msg.stream_id == 2:
                    file_started.set()
                    await gate.wait()
                else:
                    chat.set()
                ep.consume(msg)
            return receive

        async with endpoints(handler, limits=small_limits()) as (server, client, cid):
            receipt = await client.send_message(cid, 2, b"file")
            await asyncio.wait_for(file_started.wait(), 1)
            await receipt.wait_acked()
            await client.send_message(cid, 1, b"chat")
            await asyncio.wait_for(chat.wait(), 1)
            assert not gate.is_set()
            stats = server.get_stats(cid)
            assert stats.recv_buffered_bytes == 4
            assert client.get_peer_addr(cid) == server.local_addr
            await server.stop()
            assert not server._consumers and not server._consumer_tasks
    asyncio.run(run())


def test_real_udp_producer_waits_for_slow_consumer():
    async def run():
        gate = asyncio.Event()
        consumed = []

        def handler(ep):
            async def receive(msg):
                await gate.wait()
                consumed.append(msg.data)
                ep.consume(msg)
            return receive

        async with endpoints(handler, limits=small_limits()) as (server, client, cid):
            async def produce():
                for i in range(10):
                    await client.send_message(cid, 2, bytes([i]) * 16)
            producer = asyncio.create_task(produce())
            try:
                await eventually(lambda: server.get_stats(cid).recv_buffered_bytes == 32)
                await asyncio.sleep(0.05)
                assert not producer.done()
                assert client.get_stats(cid).pending_bytes <= 32
                gate.set()
                await asyncio.wait_for(producer, 3)
                await eventually(lambda: len(consumed) == 10)
                assert consumed == [bytes([i]) * 16 for i in range(10)]
            finally:
                producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)
    asyncio.run(run())


def test_async_handler_failure_resets_only_file_stream():
    async def run():
        chat = asyncio.Event()

        def handler(ep):
            async def receive(msg):
                if msg.stream_id == 2:
                    raise OSError("test disk failure")
                ep.consume(msg)
                chat.set()
            return receive

        async with endpoints(handler) as (server, client, cid):
            await client.send_message(cid, 2, b"bad file")
            await eventually(lambda: client.connections[cid].send_streams[2].cancelled)
            assert cid in server.connections and cid in client.connections
            await client.send_message(cid, 1, b"chat")
            await asyncio.wait_for(chat.wait(), 2)
    asyncio.run(run())


def test_legacy_send_has_bounded_try_enqueue_semantics():
    network = Network()
    ep = Endpoint(flow_limits=small_limits())
    ep.connections[7] = network.nodes["a"]
    assert ep.send(7, 2, b"x" * 16)
    assert ep.send(7, 2, b"y" * 16)
    assert not ep.send(7, 2, b"z")
    assert not ep.send(123, 2, b"missing connection")
    with pytest.raises(MessageTooLarge):
        ep.send(7, 2, b"x" * 17)
    with pytest.raises(ValueError):
        ep.set_message_handler(lambda msg: None)


def test_reset_accounts_for_data_that_never_arrived_and_frees_connection_credit():
    async def run():
        network = Network(drop=lambda source, pkt: source == "a" and pkt.ptype == C.PKT_DATA)
        a, b = network.nodes["a"], network.nodes["b"]
        a.send_message(2, b"never arrived")
        task = asyncio.create_task(a.cancel_stream(2))
        await asyncio.sleep(0)
        network.flush()
        await asyncio.wait_for(task, 1)
        assert b._rx_bytes == b._consumed_bytes == len(b"never arrived")
        assert b._rx_packets == b._consumed_packets == 1
        assert b.get_stats().recv_buffered_bytes == 0
    asyncio.run(run())


def test_close_unblocks_pending_reset_waiter():
    async def run():
        network = Network()
        a = network.nodes["a"]
        a.send_message(2, b"x")
        waiter = asyncio.create_task(a.cancel_stream(2))
        await asyncio.sleep(0)
        a.close()
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(waiter, 1)
    asyncio.run(run())


def test_close_inside_callback_does_not_deliver_remaining_reassembled_messages():
    network = Network(drop=lambda source, pkt: source == "a" and
                      pkt.ptype == C.PKT_DATA and pkt.seq == 0)
    a, b = network.nodes["a"], network.nodes["b"]
    got = []

    def receive(message):
        got.append(message.data)
        b.close()

    b._on_delivery = receive
    for data in (b"first", b"second", b"third"):
        a.send_message(2, data)
    network.flush()  # seq 1 and 2 buffered behind the missing seq 0.
    raw = next(raw for _, source, pkt, raw, _ in network.events
               if source == "a" and pkt.ptype == C.PKT_DATA and pkt.seq == 0)
    b.handle_packet(decode(raw))
    assert got == [b"first"]
    assert b.closed and not b._deliveries
    assert b.get_stats().recv_buffered_bytes == 0


def test_reset_retry_exhaustion_closes_connection(monkeypatch):
    monkeypatch.setattr(C, "MAX_RETRIES", 2)
    network = Network(drop=lambda source, pkt: pkt.ptype == C.PKT_RESET_ACK)
    a = network.nodes["a"]
    a.send_message(2, b"x")
    a.begin_cancel(2)
    network.flush()
    for _ in range(200):
        network.step()
        if a.closed:
            break
    assert a.closed and a.reason == "too many retries"
    assert not a._reset_out


def test_handler_can_stop_its_endpoint_without_awaiting_itself():
    async def run():
        stopped = asyncio.Event()

        def handler(ep):
            async def receive(msg):
                await ep.stop()
                stopped.set()
            return receive

        async with endpoints(handler) as (server, client, cid):
            await client.send_message(cid, 1, b"stop")
            await asyncio.wait_for(stopped.wait(), 1)
            assert server._transport is None and not server._consumers
    asyncio.run(run())


def test_stop_wakes_handshake_waiter():
    async def run():
        # Keep a UDP peer bound but silent so this does not depend on ICMP or
        # Windows' handling of packets to an unbound ephemeral port.
        loop = asyncio.get_running_loop()
        peer, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol,
                                                     local_addr=("127.0.0.1", 0))
        ep = Endpoint()
        task = None
        try:
            await ep.start(("127.0.0.1", 0))
            task = asyncio.create_task(ep.connect(peer.get_extra_info("sockname")))
            await eventually(lambda: ep._pending is not None)
            await ep.stop()
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(task, 1)
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await ep.stop()
            peer.close()
    asyncio.run(run())


@pytest.mark.parametrize("file_busy", [False, True])
def test_chat_latency_with_and_without_blocked_file(file_busy):
    async def run():
        gate = asyncio.Event()
        received = asyncio.Queue()

        def handler(ep):
            async def receive(msg):
                if msg.stream_id == 2:
                    await gate.wait()
                else:
                    received.put_nowait(msg.data)
                ep.consume(msg)
            return receive

        async with endpoints(handler, limits=small_limits()) as (server, client, cid):
            if file_busy:
                for _ in range(2):
                    await client.send_message(cid, 2, b"f" * 16)
                await eventually(lambda: server.get_stats(cid).recv_buffered_bytes == 32)
            samples = []
            loop = asyncio.get_running_loop()
            for index in range(20):
                data = str(index).encode()
                start = loop.time()
                await client.send_message(cid, 1, data)
                assert await asyncio.wait_for(received.get(), 1) == data
                samples.append(loop.time() - start)
            p50 = statistics.median(samples)
            p95 = sorted(samples)[18]
            # Internal loopback acceptance threshold set before measurement.
            assert p95 < 0.25
            assert not gate.is_set()
            print(json.dumps({"chat_file_busy": file_busy, "samples": 20,
                              "p50_seconds": p50, "p95_seconds": p95,
                              "acceptance_p95_seconds": 0.25}))
    asyncio.run(run())


def test_large_transfer_memory_does_not_scale_with_total_payload():
    peaks = []
    for megabytes in (10, 100):
        tracemalloc.start()
        network = Network(limits=FlowLimits(), record=False)
        a, b = network.nodes["a"], network.nodes["b"]
        chunk = b"x" * 8192
        digest, expected = hashlib.sha256(), hashlib.sha256()
        sent = consumed = 0
        target = megabytes * 1024 * 1024
        while consumed < target:
            while sent < target:
                try:
                    a.send_message(2, chunk)
                except QueueFull:
                    break
                sent += len(chunk)
                expected.update(chunk)
            network.step()
            # Disk-like consumer takes one chunk per scheduling step.
            if network.held["b"]:
                msg = network.held["b"].pop(0)
                digest.update(msg.data)
                consumed += len(msg.data)
                b.consume(msg)
            assert a.get_stats().pending_bytes + a.get_stats().unacked_bytes <= a.limits.stream_bytes
            assert b.get_stats().recv_buffered_bytes <= b.limits.stream_bytes
            assert network.now < 10000, "transfer stalled"
        assert digest.digest() == expected.digest()
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        peaks.append(peak)
        print(json.dumps({"payload_mib": megabytes, "tracemalloc_peak_bytes": peak,
                          "send_payload_peak": a._send_peak, "receive_payload_peak": b._recv_peak,
                          "bytes_consumed": consumed}))
    assert max(peaks) < 16 * 1024 * 1024
    assert peaks[1] < peaks[0] + 2 * 1024 * 1024
