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
from .flow import MessageTooLarge


@dataclass
class InFlight:
    """Một gói đã gửi nhưng chưa được ACK."""
    pkt: Packet
    sent_at: float
    retries: int = 0
    retry_interval: float = C.INITIAL_RTO
    deadline: float = 0.0
    gap_acks: int = 0
    fast_retransmitted: bool = False
    wire_data: bytes = b""
    # A4: ACKs from a flight that filled cwnd may grow CUBIC even as that
    # flight drains. Sparse application/credit-limited flights may not.
    cwnd_limited: bool = False


class InvalidAck(ValueError):
    """ACK/SACK refers to data outside the sequence space already sent."""


class SendStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.next_seq = 0
        self.highest_sent = -1
        self.cumulative_ack = 0
        self.cancelled = False
        self.queued_bytes = 0
        self.queued_messages = 0
        self.sent_bytes = 0
        self.receipts = {}
        self.peer_credit = None
        self.pending: Deque[Packet] = deque()       # đã cắt gói, chờ được phép gửi
        self.unacked: Dict[int, InFlight] = {}      # seq -> InFlight

    def enqueue(self, data: bytes) -> None:
        """Cắt 1 message thành các gói ≤ MAX_PAYLOAD; gói cuối gắn FLAG_EOM."""
        count = max(1, (len(data) + C.MAX_PAYLOAD - 1) // C.MAX_PAYLOAD)
        # Reserve uint32's final value for the cumulative ACK after last DATA.
        if self.next_seq + count > 0xFFFFFFFF:
            raise OverflowError("stream sequence exhausted")
        chunks = [data[i:i + C.MAX_PAYLOAD] for i in range(0, len(data), C.MAX_PAYLOAD)] or [b""]
        for i, chunk in enumerate(chunks):
            flags = C.FLAG_EOM if i == len(chunks) - 1 else 0
            self.pending.append(Packet(C.PKT_DATA, stream_id=self.stream_id,
                                       seq=self.next_seq, flags=flags, payload=chunk))
            self.next_seq += 1

    def mark_sent(self, pkt: Packet, now: float, rto: float = C.INITIAL_RTO,
                  wire_data: bytes = b"") -> None:
        self.highest_sent = pkt.seq
        self.unacked[pkt.seq] = InFlight(
            pkt, now, retry_interval=rto, deadline=now + rto, wire_data=wire_data)

    def mark_retransmitted(self, seq: int, now: float, *, timeout: bool = False) -> None:
        inf = self.unacked.get(seq)
        if inf:
            inf.sent_at = now
            inf.retries += 1
            if timeout:
                inf.retry_interval = min(inf.retry_interval * 2, C.MAX_RTO)
            inf.deadline = now + inf.retry_interval

    def on_ack(self, ack: int, sack: int) -> List[InFlight]:
        """Xóa các gói đã được ACK (tích lũy + SACK); trả về danh sách vừa được ACK (để lấy mẫu RTT)."""
        # Validate the entire frame before mutating anything. Enqueued is not sent.
        if not 0 <= ack <= self.highest_sent + 1 or not 0 <= sack < (1 << C.SACK_BITS):
            raise InvalidAck("ACK outside sent range")
        selectively_acked = [ack + 1 + i for i in range(C.SACK_BITS) if (sack >> i) & 1]
        if any(seq > self.highest_sent for seq in selectively_acked):
            raise InvalidAck("SACK outside sent range")
        self.cumulative_ack = max(self.cumulative_ack, ack)
        acked = [self.unacked.pop(s) for s in [s for s in self.unacked if s < ack]]
        for seq in selectively_acked:
            inf = self.unacked.pop(seq, None)
            if inf:
                acked.append(inf)
        # Only newly acknowledged, distinct sequences contribute evidence.
        # Each sequence leaves unacked once, so duplicate/stale ACKs cannot
        # accumulate evidence or inflate congestion/progress counters.
        for missing in self.unacked.values():
            missing.gap_acks = min(3, missing.gap_acks + sum(
                inf.pkt.seq > missing.pkt.seq for inf in acked))
        return acked

    def timed_out(self, now: float) -> List[InFlight]:
        """Per-packet deadlines, unchanged by RTT samples from other packets."""
        return [inf for inf in self.unacked.values() if now >= inf.deadline]

    def fast_retransmit_candidates(self) -> List[InFlight]:
        return [inf for inf in self.unacked.values()
                if inf.gap_acks >= 3 and not inf.fast_retransmitted and inf.retries == 0]

    def can_send(self) -> bool:
        return bool(self.pending and self.pending[0].seq < self.cumulative_ack + C.MAX_WINDOW)

    @property
    def inflight(self) -> int:
        return len(self.unacked)


class RecvStream:
    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self.expected = 0                              # seq tiếp theo cần để giao đúng thứ tự
        self.buffer: Dict[int, Tuple[int, bytes]] = {} # seq -> (flags, payload) nhận chưa đúng thứ tự
        self._fragments: List[bytes] = []
        self.fragment_bytes = 0
        self.fragment_packets = 0
        self.message_packet_counts = []
        self.received_bytes = self.consumed_bytes = 0
        self.received_packets = self.consumed_packets = 0
        self.reset_final = None
        self.stopping = False

    def on_packet(self, seq: int, flags: int, payload: bytes,
                  message_limit: int | None = None) -> List[bytes]:
        """Nhận 1 gói đã giải mã. Trả về các message hoàn chỉnh (đúng thứ tự) vừa hoàn thành, thường 0 hoặc 1."""
        self.message_packet_counts = []
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
            self.fragment_bytes += len(p)
            self.fragment_packets += 1
            if message_limit is not None and self.fragment_bytes > message_limit:
                raise MessageTooLarge("receive message exceeds limit or missing EOM")
            if f & C.FLAG_EOM:
                messages.append(b"".join(self._fragments))
                self.message_packet_counts.append(self.fragment_packets)
                self._fragments.clear()
                self.fragment_bytes = self.fragment_packets = 0
        return messages

    def ack_info(self) -> Tuple[int, int]:
        """(ack, sack) để đưa vào gói ACK."""
        sack = 0
        for i in range(C.SACK_BITS):
            if (self.expected + 1 + i) in self.buffer:
                sack |= 1 << i
        return self.expected, sack
