"""
Stream: mỗi stream có không gian seq, hàng đợi gửi và bộ đệm nhận RIÊNG.   [Người phụ trách: A]  (Bước 5, 6)

Vì mỗi stream tự sắp xếp thứ tự của mình nên gói file bị mất chỉ làm chậm chính file đó,
không chặn chat  =>  không có Head-of-Line blocking giữa các stream (yêu cầu của đề).

  SendStream : cắt message thành gói, giữ gói chưa được ACK.
  RecvStream : sắp xếp lại gói theo seq, ghép gói thành message nguyên vẹn, tạo thông tin ACK/SACK.

Quy ước ACK:  ack = seq tiếp theo đang chờ (mọi seq < ack đã nhận đủ).
              sack bit i = 1  <=>  đã nhận seq (ack + 1 + i)   (seq == ack chắc chắn đang thiếu)
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, List, Tuple

from common import constants as C
from common.packet import Packet


@dataclass
class InFlight:
    """Một gói đã gửi nhưng chưa được ACK."""
    pkt: Packet
    sent_at: float
    retries: int = 0


class SendStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.next_seq = 0
        self.pending: Deque[Packet] = deque()       # đã cắt gói, chờ được phép gửi
        self.unacked: Dict[int, InFlight] = {}      # seq -> InFlight

    def enqueue(self, data: bytes) -> None:
        """Cắt 1 message thành các gói ≤ MAX_PAYLOAD; gói cuối gắn FLAG_EOM."""
        chunks = [data[i:i + C.MAX_PAYLOAD] for i in range(0, len(data), C.MAX_PAYLOAD)] or [b""]
        for i, chunk in enumerate(chunks):
            flags = C.FLAG_EOM if i == len(chunks) - 1 else 0
            self.pending.append(Packet(C.PKT_DATA, stream_id=self.stream_id,
                                       seq=self.next_seq, flags=flags, payload=chunk))
            self.next_seq += 1

    def mark_sent(self, pkt: Packet, now: float) -> None:
        self.unacked[pkt.seq] = InFlight(pkt, now)

    def mark_retransmitted(self, seq: int, now: float) -> None:
        inf = self.unacked.get(seq)
        if inf:
            inf.sent_at = now
            inf.retries += 1

    def on_ack(self, ack: int, sack: int) -> List[InFlight]:
        """Xóa các gói đã được ACK (tích lũy + SACK); trả về danh sách vừa được ACK (để lấy mẫu RTT)."""
        acked = [self.unacked.pop(s) for s in [s for s in self.unacked if s < ack]]
        for i in range(C.SACK_BITS):
            if (sack >> i) & 1:
                inf = self.unacked.pop(ack + 1 + i, None)
                if inf:
                    acked.append(inf)
        return acked

    def timed_out(self, now: float, rto: float) -> List[InFlight]:
        """Các gói đã gửi quá `rto` giây mà chưa có ACK (ứng viên gửi lại)."""
        return [inf for inf in self.unacked.values() if now - inf.sent_at > rto]

    @property
    def inflight(self) -> int:
        return len(self.unacked)


class RecvStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.expected = 0                              # seq tiếp theo cần để giao đúng thứ tự
        self.buffer: Dict[int, Tuple[int, bytes]] = {} # seq -> (flags, payload) nhận chưa đúng thứ tự
        self._fragments: List[bytes] = []

    def on_packet(self, seq: int, flags: int, payload: bytes) -> List[bytes]:
        """Nhận 1 gói đã giải mã. Trả về các message hoàn chỉnh (đúng thứ tự) vừa hoàn thành, thường 0 hoặc 1."""
        if seq < self.expected or seq in self.buffer:
            return []                                  # gói trùng (do gửi lại)
        if seq >= self.expected + C.MAX_WINDOW:
            return []                                  # quá xa cửa sổ nhận: bỏ, bên gửi sẽ gửi lại
        self.buffer[seq] = (flags, payload)
        messages: List[bytes] = []
        while self.expected in self.buffer:
            f, p = self.buffer.pop(self.expected)
            self.expected += 1
            self._fragments.append(p)
            if f & C.FLAG_EOM:
                messages.append(b"".join(self._fragments))
                self._fragments.clear()
        return messages

    def ack_info(self) -> Tuple[int, int]:
        """(ack, sack) để đưa vào gói ACK."""
        sack = 0
        for i in range(C.SACK_BITS):
            if (self.expected + 1 + i) in self.buffer:
                sack |= 1 << i
        return self.expected, sack
