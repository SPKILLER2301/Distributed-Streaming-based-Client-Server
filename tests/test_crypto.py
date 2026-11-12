"""A6: authenticated wire, directional/session keys, nonce and replay limits."""
import struct

import pytest

from common import constants as C
from common.packet import Packet, decode
from transport.crypto import CryptoError, KeyExchange, SessionCipher
from transport.flow import ConnectionClosed, StreamLimitError
from tests.test_flow_control import Network
from tests.test_recovery import Sender


def ciphers():
    a, b = KeyExchange(), KeyExchange()
    return a.derive_cipher(b.public_bytes, True), b.derive_cipher(a.public_bytes, False)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("payload", [b"", b"secret", b"x" * C.MAX_PAYLOAD])
def test_both_directions_and_separate_control_nonce_domain(reverse, payload):
    a, b = ciphers()
    sender, receiver = (b, a) if reverse else (a, b)
    data = sender.encrypt(2, 0, b"header", payload)
    control = sender.encrypt_control(0, b"header", payload)
    assert receiver.decrypt(2, 0, b"header", data) == payload
    assert receiver.decrypt_control(0, b"header", control) == payload
    assert data != control
    with pytest.raises(CryptoError):
        receiver.decrypt_control(0, b"header", data)
    with pytest.raises(CryptoError):
        sender.decrypt(2, 0, b"header", data)


@pytest.mark.parametrize("key", [b"", b"x" * 31, b"x" * 33, bytes(32), b"\x01" + bytes(31)])
def test_invalid_public_keys_fail_with_crypto_error(key):
    with pytest.raises(CryptoError):
        KeyExchange().derive_cipher(key, True)


@pytest.mark.parametrize("sid,seq", [(-1, 0), (65536, 0), (True, 0), (0, -1), (0, 1 << 32), (0, False)])
def test_data_nonce_rejects_out_of_range_fields(sid, seq):
    with pytest.raises(ValueError):
        SessionCipher._nonce(sid, seq)


@pytest.mark.parametrize("counter", [-1, 1 << 64, True, 1.0])
def test_control_nonce_rejects_counter_wrap(counter):
    with pytest.raises(ValueError):
        SessionCipher._control_nonce(counter)


def test_nonce_layout_boundaries_are_distinct():
    data = {SessionCipher._nonce(sid, seq) for sid in (0, 1, 65535)
            for seq in (0, 1, 0xFFFFFFFE)}
    control = {SessionCipher._control_nonce(n) for n in (0, 1, (1 << 64) - 1)}
    assert len(data) == 9 and len(control) == 3
    assert all(len(nonce) == 12 for nonce in data | control)
    assert data.isdisjoint(control)
    assert SessionCipher._nonce(1, 2).hex() == "000001000000020000000000"
    assert SessionCipher._control_nonce(1).hex() == "010000000000000001000000"


def test_new_handshake_rejects_old_session_ciphertext():
    old_sender, old_receiver = ciphers()
    new_sender, new_receiver = ciphers()
    old = old_sender.encrypt(2, 0, b"same header", b"payload")
    new = new_sender.encrypt(2, 0, b"same header", b"payload")
    assert old != new
    assert old_receiver.decrypt(2, 0, b"same header", old) == b"payload"
    with pytest.raises(CryptoError):
        new_receiver.decrypt(2, 0, b"same header", old)


@pytest.mark.parametrize("offset", [2, 4, 8, 12, 16, 20, 23, -1])
def test_tampered_data_never_delivers_or_refreshes_liveness(offset):
    net = Network(drop=lambda source, pkt: source == "a")
    a, b = net.nodes["a"], net.nodes["b"]
    a.send_message(2, b"secret")
    raw = net.events[0][3]
    bad = bytearray(raw)
    bad[offset] ^= 1
    net.now += 0.1
    before = b.last_recv, b._rx_bytes, b._control_counter
    b.handle_packet(decode(bytes(bad)))
    assert (b.last_recv, b._rx_bytes, b._control_counter) == before
    assert not net.held["b"] and not b.closed
    b.handle_packet(decode(raw))
    assert [m.data for m in net.held["b"]] == [b"secret"]


def test_duplicate_data_delivers_once_but_acks_again_with_fresh_counter():
    net = Network(drop=lambda source, pkt: source == "a")
    a, b = net.nodes["a"], net.nodes["b"]
    a.send_message(2, b"secret")
    raw = net.events[0][3]
    b.handle_packet(decode(raw))
    net.now += 0.1
    b.handle_packet(decode(raw))
    acks = [wire for _, source, pkt, wire, _ in net.events
            if source == "b" and pkt.ptype == C.PKT_ACK]
    assert len(acks) == 2 and acks[0] != acks[1]
    assert len(net.held["b"]) == 1
    assert [struct.unpack("!Q", decode(wire).payload[:8])[0] for wire in acks] == [0, 1]


def test_retransmit_does_not_call_encrypt_again_and_ack_releases_cache(monkeypatch):
    sender = Sender()
    calls = []
    encrypt = sender.conn.cipher.encrypt

    def record(sid, seq, aad, plain):
        calls.append((sid, seq, aad, plain))
        return encrypt(sid, seq, aad, plain)

    monkeypatch.setattr(sender.conn.cipher, "encrypt", record)
    receipt = sender.conn.send_message(2, b"immutable")
    original = sender.wire[0]
    sender.tick(C.INITIAL_RTO)
    assert len(calls) == 1 and sender.wire[-1] == original
    sender.ack(1)
    assert receipt.acked_bytes == len(b"immutable")
    assert sender.conn.get_stats().wire_cache_bytes == 0


def test_control_replay_window_accepts_reorder_inside_256_but_not_old_or_duplicate():
    net = Network()
    a, b = net.nodes["a"], net.nodes["b"]
    raw = [b._encode_packet(Packet(C.PKT_PING)) for _ in range(257)]
    for index, accepted in [(256, True), (1, True), (0, False), (255, True), (255, False)]:
        before = a.last_recv
        net.now += 0.1
        a.handle_packet(decode(raw[index]))
        assert (a.last_recv != before) is accepted
    assert a._peer_control_counter == 256


def test_bad_tag_with_high_counter_cannot_poison_replay_window():
    net = Network()
    a, b = net.nodes["a"], net.nodes["b"]
    first = b._encode_packet(Packet(C.PKT_PING))
    b._control_counter = 1000
    bad = bytearray(b._encode_packet(Packet(C.PKT_PING)))
    bad[-1] ^= 1
    a.handle_packet(decode(bytes(bad)))
    assert a._peer_control_counter == -1
    a.handle_packet(decode(first))
    assert a._peer_control_counter == 0


@pytest.mark.parametrize("ptype,body", [(C.PKT_ACK, b""), (C.PKT_CLOSE, b"\x00"),
    (C.PKT_CREDIT, struct.pack("!6Q", 100, 200, 180, 30, 60, 50))])
def test_forged_control_cannot_ack_close_or_change_credit(ptype, body):
    net = Network(drop=lambda source, pkt: True)
    a, b = net.nodes["a"], net.nodes["b"]
    a.send_message(2, b"payload")
    raw = bytearray(b._encode_packet(Packet(ptype, stream_id=0 if ptype == C.PKT_CLOSE else 2,
                                            ack=1 if ptype == C.PKT_ACK else 0, payload=body)))
    raw[-1] ^= 1
    before = a.last_recv, a._peer_conn_bytes, a.stats["acked_payload_bytes"], a.cc.cwnd
    a.handle_packet(decode(bytes(raw)))
    assert not a.closed and a._inflight_total() == 1
    assert (a.last_recv, a._peer_conn_bytes, a.stats["acked_payload_bytes"], a.cc.cwnd) == before


@pytest.mark.parametrize("field,value", [("flags", 1), ("seq", 1), ("ack", 1), ("sack", 1), ("stream_id", 1)])
def test_authenticated_unused_control_fields_do_not_mutate_session(field, value):
    net = Network()
    a, b = net.nodes["a"], net.nodes["b"]
    raw = b._encode_packet(Packet(C.PKT_PING, **{field: value}))
    before = a.last_recv, a._peer_control_counter
    net.now += 0.1
    a.handle_packet(decode(raw))
    assert (a.last_recv, a._peer_control_counter) == before


def test_counter_exhaustion_closes_without_wrap_and_wakes_receipt():
    sender = Sender()
    receipt = sender.conn.send_message(2, b"unacked")
    sender.conn._control_counter = (1 << 64) - 1
    sender.conn._transmit(Packet(C.PKT_PING))
    sender.conn.close()
    counters = [struct.unpack("!Q", p.payload[:8])[0] for p in map(decode, sender.wire)
                if p.ptype != C.PKT_DATA]
    assert counters == [(1 << 64) - 1]
    assert sender.closed == ["control counter exhausted"]
    assert isinstance(receipt._error, ConnectionClosed)
    assert not sender.conn._control_pending and not sender.conn.send_streams


def test_counter_exhaustion_during_credit_probe_does_not_repopulate_closed_state():
    sender = Sender()
    stream = sender.conn._send_stream(2)
    stream.peer_credit = (0, 0)
    sender.send(1)
    sender.conn._control_counter = 1 << 64
    sender.tick()
    sender.tick(C.INITIAL_RTO)
    assert sender.conn.closed and not sender.conn._probes


def test_last_stream_id_is_usable_once_but_cannot_wrap_or_reuse_after_reset():
    sender = Sender()
    sender.conn._next_stream_id = 65535
    assert sender.conn.open_stream() == 65535
    sender.conn.begin_cancel(65535)
    with pytest.raises((StreamLimitError, ValueError)):
        sender.conn.open_stream()
    assert sender.conn.send_streams[65535].cancelled


@pytest.mark.parametrize("changes", [{"ack": 1}, {"sack": 1}, {"seq": 0xFFFFFFFF}])
def test_authenticated_data_with_invalid_reserved_fields_does_not_refresh_peer(changes):
    net = Network()
    a, b = net.nodes["a"], net.nodes["b"]
    raw = a._encode_packet(Packet(C.PKT_DATA, stream_id=2, flags=C.FLAG_EOM,
                                 payload=b"payload", **changes))
    before = b.last_recv, b._rx_bytes
    net.now += 0.1
    b.handle_packet(decode(raw))
    assert (b.last_recv, b._rx_bytes) == before and not net.held["b"]
