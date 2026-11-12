"""Kiểm thử đóng/mở gói (common/packet.py).   [A]"""
import pytest

from common import constants as C
from common.packet import Packet, PacketError, decode, encode


def test_roundtrip():
    p = Packet(C.PKT_DATA, conn_id=7, stream_id=3, seq=42, ack=5, sack=0b101, flags=C.FLAG_EOM, payload=b"hello")
    q = decode(encode(p))
    assert q == p


def test_header_size_constant():
    assert len(Packet(C.PKT_PING).header_bytes()) == C.HEADER_SIZE


def test_too_short():
    with pytest.raises(PacketError):
        decode(b"\x01\x02")


def test_bad_version():
    raw = bytearray(encode(Packet(C.PKT_PING)))
    raw[0] = 99
    with pytest.raises(PacketError):
        decode(bytes(raw))


def test_length_mismatch():
    with pytest.raises(PacketError):
        decode(encode(Packet(C.PKT_DATA, payload=b"abc")) + b"xx")


def test_oversized_datagram_is_rejected_even_with_matching_length():
    with pytest.raises(PacketError):
        decode(encode(Packet(C.PKT_DATA, payload=b"x" * (C.MAX_DATAGRAM - C.HEADER_SIZE + 1))))


@pytest.mark.parametrize("ptype", [C.PKT_STOP_STREAM + 1, 255])
def test_unknown_packet_type_is_rejected(ptype):
    with pytest.raises(PacketError):
        decode(encode(Packet(ptype)))


@pytest.mark.parametrize("raw", [b"", bytes(C.HEADER_SIZE - 1), bytes(C.HEADER_SIZE), bytes(C.MAX_DATAGRAM)])
def test_arbitrary_malformed_bytes_raise_packet_error(raw):
    with pytest.raises(PacketError):
        decode(raw)
