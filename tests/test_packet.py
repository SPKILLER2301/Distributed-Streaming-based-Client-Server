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
