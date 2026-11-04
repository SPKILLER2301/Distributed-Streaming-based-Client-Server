"""
Đóng/mở gói tin nhị phân của tầng transport.             [Người phụ trách: A]  (Bước 1, 2)

Định dạng header xem docs/protocol.md. Module này KHÔNG biết gì về mã hóa hay socket:
chỉ chuyển qua lại giữa đối tượng Packet <-> bytes.

Cách dùng:
    data = encode(Packet(PKT_PING, conn_id=7))
    pkt  = decode(data)          # raise PacketError nếu gói hỏng -> bên gọi phải bắt và bỏ gói
Với gói DATA đã mã hóa, connection.py tự dựng header bằng Packet.header_bytes(payload_len)
để dùng làm AAD cho AES-GCM.
"""
from __future__ import annotations

import struct
from dataclasses import dataclass

from .constants import HEADER_SIZE, PROTOCOL_VERSION

# version, ptype, flags, stream_id, conn_id, seq, ack, sack, length
HEADER_FORMAT = "!BBBHIIIIH"
assert struct.calcsize(HEADER_FORMAT) == HEADER_SIZE, "HEADER_SIZE trong constants.py không khớp header"


class PacketError(Exception):
    """Gói tin sai định dạng / sai phiên bản -> bỏ qua gói, KHÔNG được làm crash server."""


@dataclass
class Packet:
    ptype: int
    conn_id: int = 0
    stream_id: int = 0
    seq: int = 0
    ack: int = 0
    sack: int = 0
    flags: int = 0
    payload: bytes = b""
    version: int = PROTOCOL_VERSION

    def header_bytes(self, payload_len: int | None = None) -> bytes:
        """Dựng 23 byte header. payload_len mặc định = len(self.payload)."""
        if payload_len is None:
            payload_len = len(self.payload)
        return struct.pack(HEADER_FORMAT, self.version, self.ptype, self.flags, self.stream_id,
                           self.conn_id, self.seq, self.ack, self.sack, payload_len)


def encode(pkt: Packet) -> bytes:
    """Gói -> bytes (payload giữ nguyên, không mã hóa)."""
    return pkt.header_bytes() + pkt.payload


def decode(data: bytes) -> Packet:
    """bytes -> Packet. Raise PacketError nếu hỏng."""
    if len(data) < HEADER_SIZE:
        raise PacketError("gói quá ngắn")
    (version, ptype, flags, stream_id, conn_id,
     seq, ack, sack, length) = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])
    if version != PROTOCOL_VERSION:
        raise PacketError(f"sai phiên bản {version}")
    payload = data[HEADER_SIZE:]
    if len(payload) != length:
        raise PacketError("length không khớp payload")
    return Packet(ptype, conn_id, stream_id, seq, ack, sack, flags, payload, version)
