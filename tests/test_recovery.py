"""A3 deterministic recovery tests: virtual monotonic clock, no sleeps/socket."""
from collections import deque

import pytest

from common import constants as C
from common.packet import Packet, decode
from transport.connection import Connection
from transport.crypto import KeyExchange
from transport.stream import InvalidAck, RecvStream, SendStream


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class Sender:
    def __init__(self, algorithm="cubic"):
        self.clock, self.wire, self.closed = Clock(), [], []
        a, b = KeyExchange(), KeyExchange()
        self.conn = Connection(7, ("peer", 9000), a.derive_cipher(b.public_bytes, True),
                               lambda raw, addr: self.wire.append(raw),
                               lambda *args: None, lambda cid, reason: self.closed.append(reason),
                               clock=self.clock, congestion_algorithm=algorithm)

    def send(self, count, sid=2):
        for _ in range(count):
            self.conn.send_message(sid, b"x")

    def ack(self, ack, sack=0, sid=2):
        # This helper tests recovery after control authentication. Wire-level
        # authenticated/replayed controls are exercised by A5 tests separately.
        self.conn._on_ack(Packet(C.PKT_ACK, stream_id=sid, ack=ack, sack=sack))

    def tick(self, seconds=0):
        self.clock.advance(seconds)
        self.conn.on_tick(self.clock())

    def data_packets(self):
        return [decode(raw) for raw in self.wire if decode(raw).ptype == C.PKT_DATA]


def sent_stream(count=5):
    stream = SendStream(2)
    for _ in range(count):
        stream.enqueue(b"x")
        stream.mark_sent(stream.pending.popleft(), 100)
    return stream


@pytest.fixture(params=["cubic", "aimd"])
def algorithm(request):
    return request.param


@pytest.mark.parametrize("ack,sack", [(6, 0), (0, 1 << 4), (-1, 0), (0, -1), (0, 1 << 32)])
def test_invalid_ack_is_atomic(ack, sack):
    stream = sent_stream()
    before = dict(stream.unacked)
    with pytest.raises(InvalidAck):
        stream.on_ack(ack, sack)
    assert stream.unacked == before and stream.cumulative_ack == 0


def test_ack_cannot_confirm_enqueued_but_unsent_data():
    stream = sent_stream(2)
    stream.enqueue(b"pending")
    with pytest.raises(InvalidAck):
        stream.on_ack(3, 0)
    assert len(stream.unacked) == 2 and len(stream.pending) == 1


def test_stale_ack_can_add_new_sack_without_moving_window_back():
    stream = sent_stream()
    assert [inf.pkt.seq for inf in stream.on_ack(2, 0)] == [0, 1]
    assert [inf.pkt.seq for inf in stream.on_ack(0, 1 << 2)] == [3]
    assert stream.cumulative_ack == 2
    assert set(stream.unacked) == {2, 4}


def test_fast_retransmit_requires_three_distinct_sequences():
    sender = Sender()
    sender.send(5)
    sender.clock.advance(0.01)
    sender.ack(1, 0b001)
    for _ in range(10):
        sender.ack(1, 0b001)
    assert sender.conn.stats["retransmitted"] == 0
    sender.ack(1, 0b010)
    assert sender.conn.stats["retransmitted"] == 0
    sender.ack(1, 0b100)
    assert [pkt.seq for pkt in sender.data_packets()] == [0, 1, 2, 3, 4, 1]
    assert sender.clock() < 100.5  # Before original RTO.
    before = dict(sender.conn.stats)
    cwnd = sender.conn.cc.cwnd
    for _ in range(10):
        sender.ack(1, 0b111)
    assert sender.conn.stats == before and sender.conn.cc.cwnd == cwnd


def test_invalid_ack_does_not_inflate_cwnd_or_ack_progress(algorithm):
    sender = Sender(algorithm)
    sender.send(5)
    sender.ack(6)
    assert sender.conn.stats["invalid_acks"] == 1
    assert sender.conn.stats["acked_payload_bytes"] == 0
    assert sender.conn.cc.cwnd == C.INITIAL_CWND
    assert len(sender.conn.send_streams[2].unacked) == 5


def test_sack_does_not_allow_sending_past_receive_sequence_window():
    sender = Sender()
    sender.conn.cc.cwnd = C.MAX_WINDOW
    for _ in range(80):
        sender.conn.send_message(2, b"x" * C.MAX_PAYLOAD)
    assert len(sender.data_packets()) == 64
    sender.clock.advance(0.01)
    sender.ack(0, 0xFFFFFFFF)
    assert max(pkt.seq for pkt in sender.data_packets()) == 63
    assert sender.conn.send_streams[2].pending[0].seq == 64
    sender.ack(1)
    assert max(pkt.seq for pkt in sender.data_packets()) == 64


def test_recovery_snapshot_uses_separate_boundaries_for_each_stream(algorithm):
    sender = Sender(algorithm)
    sender.send(5, sid=2)
    sender.send(5, sid=3)
    sender.clock.advance(0.01)
    sender.ack(1, 0b111, sid=2)
    assert sender.conn._recovery == {2: 4, 3: 4}
    sender.ack(5, sid=2)
    assert sender.conn._recovery is not None
    sender.ack(1, 0b111, sid=3)
    assert sender.conn.stats["loss_reductions"] == 1
    sender.ack(5, sid=3)
    assert sender.conn._recovery is None
    sender.send(5, sid=2)
    sender.ack(6, 0b111, sid=2)
    assert sender.conn.stats["recovery_epochs"] == 2
    assert sender.conn.stats["loss_reductions"] == 2


def test_recovery_exits_at_snapshot_even_with_newer_data_outstanding(algorithm):
    sender = Sender(algorithm)
    sender.send(10)
    sender.clock.advance(0.01)
    # Leave two old packets outstanding: both controllers then have room
    # for newer DATA despite their different loss-reduction factors.
    sender.ack(5, 0b111)
    sender.send(10)
    assert sender.conn._recovery == {2: 9}
    assert sender.conn.send_streams[2].highest_sent > 9
    sender.ack(10)
    assert sender.conn._recovery is None
    assert sender.conn.send_streams[2].unacked  # Newly sent DATA remains.
    assert sender.conn.stats["loss_reductions"] == 1
    sender.ack(11, 0b111)
    assert sender.conn.stats["recovery_epochs"] == 2


def test_each_packet_keeps_its_rto_snapshot():
    sender = Sender()
    sender.send(1)
    sender.conn.rtt.update(0.01)
    sender.send(1)
    stream = sender.conn.send_streams[2]
    sender.tick(C.MIN_RTO + 0.000001)
    assert stream.unacked[0].retries == 0
    assert stream.unacked[0].deadline == 100 + C.INITIAL_RTO
    assert stream.unacked[1].retries == 1
    assert stream.unacked[1].retry_interval == 2 * C.MIN_RTO


def test_timeout_batch_is_paced_and_backoff_does_not_multiply_per_packet(algorithm):
    sender = Sender(algorithm)
    sender.send(10)
    sender.tick(C.INITIAL_RTO)
    assert sender.conn.stats["timeout_count"] == 1
    assert sender.conn.stats["retransmitted"] == 1
    assert sender.conn.cc.cwnd == 1
    for _ in range(10):
        sender.tick()
    assert sender.conn.stats["retransmitted"] == 1
    for _ in range(9):
        sender.tick(C.TICK_INTERVAL + 0.000001)
    assert sender.conn.stats["retransmitted"] == 10
    assert sender.conn.stats["timeout_reductions"] == 1
    assert sender.conn.rtt.rto == C.INITIAL_RTO
    assert {inf.retry_interval for inf in sender.conn.send_streams[2].unacked.values()} == {1.0}


def test_tail_loss_per_packet_backoff_is_capped_and_ignores_later_rtt_updates(monkeypatch):
    monkeypatch.setattr(C, "PEER_TIMEOUT", 1000)
    sender = Sender()
    sender.send(1)
    stream = sender.conn.send_streams[2]
    inf = stream.unacked[0]
    sender.conn.rtt.update(0.01)  # Does not retroactively move inf.deadline.
    intervals = []
    for _ in range(6):
        sender.clock.now = inf.deadline
        sender.tick()
        intervals.append(inf.retry_interval)
    assert intervals == [1, 2, 4, 5, 5, 5]
    assert sender.conn.stats["timeout_reductions"] == 1
    assert sender.conn.stats["timeout_retransmitted"] == 6


def test_fast_loss_can_escalate_to_timeout_only_once(monkeypatch, algorithm):
    monkeypatch.setattr(C, "PEER_TIMEOUT", 1000)
    sender = Sender(algorithm)
    sender.send(5)
    sender.clock.advance(0.01)
    sender.ack(1, 0b111)
    inf = sender.conn.send_streams[2].unacked[1]
    for _ in range(2):
        sender.clock.now = inf.deadline
        sender.tick()
    assert sender.conn.stats["loss_reductions"] == 1
    assert sender.conn.stats["timeout_reductions"] == 1
    assert sender.conn.stats["fast_retransmitted"] == 1
    assert sender.conn.stats["timeout_retransmitted"] == 2


def test_ack_removes_queued_retransmit_and_ends_recovery():
    sender = Sender()
    sender.send(10)
    sender.tick(C.INITIAL_RTO)
    assert len(sender.conn._retransmit_pending) == 9
    sender.ack(10)
    assert not sender.conn._retransmit_pending and sender.conn._recovery is None
    sender.tick(0.1)
    assert sender.conn.stats["retransmitted"] == 1
    assert sender.conn.stats["acked_payload_bytes"] == 10


def test_retransmitted_packet_is_not_used_for_rtt_and_bytes_are_immutable():
    sender = Sender()
    sender.send(1)
    original = sender.wire[0]
    sender.tick(C.INITIAL_RTO)
    assert sender.wire[-1] == original
    sender.ack(1)
    assert sender.conn.rtt.srtt is None
    assert sender.conn.stats["acked_packets"] == 1
    assert sender.conn.stats["acked_payload_bytes"] == 1
    sender.ack(1)
    assert sender.conn.stats["acked_payload_bytes"] == 1


def test_retry_exhaustion_closes_once_and_cleans_all_buffers(monkeypatch):
    monkeypatch.setattr(C, "MAX_RETRIES", 2)
    sender = Sender()
    sender.send(20)
    stream = sender.conn.send_streams[2]
    sender.conn.recv_streams[3] = RecvStream(3)
    for _ in range(250):
        sender.tick(0.021)
        if sender.conn.closed:
            break
    assert sender.closed == ["too many retries"]
    assert sender.conn.closed
    assert not sender.conn.send_streams and not sender.conn.recv_streams
    assert not stream.pending and not stream.unacked
    assert not sender.conn._retransmit_pending
    sender.conn.close()
    sender.tick()
    assert len(sender.closed) == 1


def test_sequence_overflow_is_atomic_and_closes_before_wrap():
    stream = SendStream(2)
    stream.next_seq = 0xFFFFFFFE
    stream.enqueue(b"last")
    assert stream.pending[0].seq == 0xFFFFFFFE
    with pytest.raises(OverflowError):
        stream.enqueue(b"must not wrap")
    assert len(stream.pending) == 1
    sender = Sender()
    sender.conn.send_streams[2] = stream
    with pytest.raises(ConnectionError, match="sequence exhausted"):
        sender.conn.send_message(2, b"no")
    assert sender.closed == ["sequence exhausted"]


class Pair:
    """Two real encrypted Connections, queued delivery, and packet drop rules."""
    def __init__(self, drop=None, algorithm="cubic"):
        self.clock, self.queue = Clock(), deque()
        self.trace, self.attempts = [], {}
        self.received = {"client": [], "server": []}
        self.connections = {}
        a, b = KeyExchange(), KeyExchange()
        for name, cipher in (("client", a.derive_cipher(b.public_bytes, True)),
                             ("server", b.derive_cipher(a.public_bytes, False))):
            def send(raw, addr, source=name):
                pkt = decode(raw)
                key = (source, pkt.ptype, pkt.stream_id, pkt.seq, pkt.ack, pkt.sack)
                attempt = self.attempts.get(key, 0) + 1
                self.attempts[key] = attempt
                dropped = bool(drop and drop(source, pkt, attempt))
                self.trace.append((self.clock(), source, pkt, dropped, raw))
                if not dropped:
                    self.queue.append(("server" if source == "client" else "client", raw))

            self.connections[name] = Connection(
                7, ("peer", 9000), cipher, send,
                lambda cid, sid, data, dest=name: self.received[dest].append((sid, data)),
                lambda *args: None, clock=self.clock, congestion_algorithm=algorithm)

    def flush(self):
        count = 0
        while self.queue:
            destination, raw = self.queue.popleft()
            self.connections[destination].handle_packet(decode(raw))
            count += 1
            assert count < 10000, "packet loop did not quiesce"

    def advance(self, seconds):
        self.clock.advance(seconds)
        for conn in self.connections.values():
            conn.on_tick(self.clock())
        self.flush()


@pytest.mark.parametrize("missing", [(1,), (1, 2)])
def test_targeted_data_loss_recovers_in_order_exactly_once(missing, algorithm):
    pair = Pair(lambda source, pkt, n: source == "client" and pkt.ptype == C.PKT_DATA
                and pkt.seq in missing and n == 1, algorithm=algorithm)
    client = pair.connections["client"]
    expected = [bytes([i]) for i in range(8)]
    for data in expected:
        client.send_message(2, data)
    pair.flush()
    for _ in range(20):
        pair.advance(0.021)
    assert pair.received["server"] == [(2, data) for data in expected]
    assert not client.send_streams[2].unacked
    assert client.stats["fast_retransmitted"] == len(missing)
    assert client.stats["timeout_retransmitted"] == 0
    assert client.stats["loss_reductions"] == 1


@pytest.mark.parametrize("kind", ["ack", "tail", "retransmit"])
def test_ack_tail_and_retransmit_loss_use_rto_without_duplicate_delivery(kind, algorithm):
    def drop(source, pkt, attempt):
        if kind == "ack":
            return source == "server" and pkt.ptype == C.PKT_ACK and attempt == 1
        if kind == "tail":
            return source == "client" and pkt.ptype == C.PKT_DATA and attempt == 1
        return source == "client" and pkt.ptype == C.PKT_DATA and attempt <= 2

    pair = Pair(drop, algorithm=algorithm)
    client = pair.connections["client"]
    client.send_message(2, b"payload")
    pair.flush()
    for _ in range(100):
        pair.advance(0.021)
        if not client.send_streams[2].unacked:
            break
    assert pair.received["server"] == [(2, b"payload")]
    assert not client.send_streams[2].unacked
    assert client.stats["timeout_retransmitted"] == (2 if kind == "retransmit" else 1)
    assert client.stats["acked_payload_bytes"] == len(b"payload")
    wire = [raw for _, source, pkt, _, raw in pair.trace
            if source == "client" and pkt.ptype == C.PKT_DATA]
    assert len(set(wire)) == 1
