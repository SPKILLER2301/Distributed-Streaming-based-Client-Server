"""
Một kết nối tin cậy giữa 2 đầu (qua UDP).                 [Người phụ trách: A]  (Bước 2, 6, 7)

Connection KHÔNG sở hữu socket: nó nhận hàm `sendto(data, addr)` từ Endpoint, và báo lên trên qua
`on_message(conn_id, stream_id, data)` / `on_closed(conn_id, reason)`.

ĐÃ CÀI SẴN (chạy được trên mạng không mất gói):
  - Cắt message thành gói, mã hóa AES-GCM, gửi theo cửa sổ cwnd (round-robin giữa các stream)
  - Nhận, giải mã, sắp xếp lại, ghép message, gửi ACK (kèm SACK) cho mỗi gói DATA
  - Xử lý ACK: xóa gói đã ACK, cập nhật RTT, tăng cwnd
  - Keepalive PING/PONG và đóng kết nối khi im lặng quá PEER_TIMEOUT

  - Gửi lại theo timeout (RTO + backoff), quá MAX_RETRIES thì đóng kết nối

A3: distinct-SACK fast retransmit, recovery epochs, per-packet backoff,
one retransmit per TICK_INTERVAL and immutable encrypted DATA bytes.
A4 adds CUBIC; A5 adds bounded queues/credit and priority scheduling.
"""
from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import Callable, Dict, Tuple

from common import constants as C
from common.packet import Packet, encode

from .congestion import CongestionController, RttEstimator
from .crypto import CryptoError, SessionCipher
from .stream import InvalidAck, RecvStream, SendStream

log = logging.getLogger(__name__)
Addr = Tuple[str, int]


class Connection:
    def __init__(self, conn_id: int, remote_addr: Addr, cipher: SessionCipher,
                 sendto: Callable[[bytes, Addr], None],
                 on_message: Callable[[int, int, bytes], None],
                 on_closed: Callable[[int, str], None], *,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.conn_id = conn_id
        self.remote_addr = remote_addr
        self.cipher = cipher
        self._sendto = sendto
        self._on_message = on_message
        self._on_closed = on_closed
        self._clock = clock

        self.send_streams: Dict[int, SendStream] = {}
        self.recv_streams: Dict[int, RecvStream] = {}
        self.rtt = RttEstimator()
        self.cc = CongestionController()
        self.closed = False
        now = self._clock()
        self.last_recv = now
        self.last_send = now
        self._next_stream_id = C.STREAM_FILE_BASE
        self.stats = {"sent": 0, "retransmitted": 0, "sent_payload_bytes": 0,
                      "acked_packets": 0, "acked_payload_bytes": 0,
                      "fast_retransmitted": 0, "timeout_retransmitted": 0,
                      "timeout_count": 0, "invalid_acks": 0,
                      "recovery_epochs": 0, "loss_reductions": 0,
                      "timeout_reductions": 0}
        self._retransmit_pending: OrderedDict[tuple[int, int], str] = OrderedDict()
        self._next_retransmit_at = now
        self._recovery: Dict[int, int] | None = None
        self._recovery_timeout = False

    # ------------------------------------------------------------------ API cho Endpoint
    def open_stream(self) -> int:
        """Cấp stream_id mới cho chiều gửi của mình (dùng cho mỗi file)."""
        sid = self._next_stream_id
        self._next_stream_id += 1
        return sid

    def send_message(self, stream_id: int, data: bytes) -> None:
        if self.closed:
            raise ConnectionError("kết nối đã đóng")
        stream = self.send_streams.setdefault(stream_id, SendStream(stream_id))
        try:
            stream.enqueue(data)
        except OverflowError as exc:
            self._finish("sequence exhausted")
            raise ConnectionError("sequence exhausted") from exc
        self._pump()

    def close(self) -> None:
        if self.closed:
            return
        self._transmit(Packet(C.PKT_CLOSE))        # cố gắng báo cho đối phương (không đảm bảo)
        self._finish("closed locally")

    # ------------------------------------------------------------------ nhận gói
    def handle_packet(self, pkt: Packet) -> None:
        if self.closed:
            return
        self.last_recv = self._clock()
        if pkt.ptype == C.PKT_DATA:
            self._on_data(pkt)
        elif pkt.ptype == C.PKT_ACK:
            self._on_ack(pkt)
        elif pkt.ptype == C.PKT_PING:
            self._transmit(Packet(C.PKT_PONG))
        elif pkt.ptype == C.PKT_CLOSE:
            self._finish("closed by peer")
        # PKT_PONG: chỉ cần cập nhật last_recv ở trên

    def _on_data(self, pkt: Packet) -> None:
        aad = pkt.header_bytes()                    # header dựng lại giống hệt bên gửi
        try:
            plain = self.cipher.decrypt(pkt.stream_id, pkt.seq, aad, pkt.payload)
        except CryptoError:
            log.debug("bỏ gói không giải mã được (conn %s)", self.conn_id)
            return
        rs = self.recv_streams.setdefault(pkt.stream_id, RecvStream(pkt.stream_id))
        messages = rs.on_packet(pkt.seq, pkt.flags, plain)
        ack, sack = rs.ack_info()
        self._transmit(Packet(C.PKT_ACK, stream_id=pkt.stream_id, ack=ack, sack=sack))
        for msg in messages:                        # giao lên trên SAU khi đã ACK
            try:
                self._on_message(self.conn_id, pkt.stream_id, msg)
            except Exception:                       # lỗi của tầng ứng dụng không được làm sập transport
                log.exception("on_message lỗi")

    def _on_ack(self, pkt: Packet) -> None:
        stream = self.send_streams.get(pkt.stream_id)
        if stream is None:
            return
        now = self._clock()
        was_recovering = self._recovery is not None
        try:
            acked = stream.on_ack(pkt.ack, pkt.sack)
        except InvalidAck:
            self.stats["invalid_acks"] += 1
            return
        for inf in acked:
            self._retransmit_pending.pop((pkt.stream_id, inf.pkt.seq), None)
            if inf.retries == 0:                    # Karn: chỉ đo RTT từ gói chưa gửi lại
                self.rtt.update(now - inf.sent_at)
        self.stats["acked_packets"] += len(acked)
        self.stats["acked_payload_bytes"] += sum(len(inf.pkt.payload) for inf in acked)
        self._maybe_end_recovery()
        for inf in stream.fast_retransmit_candidates():
            inf.fast_retransmitted = True
            self._queue_retransmit(pkt.stream_id, inf.pkt.seq, "fast")
            self._enter_recovery(timeout=False)
        # Do not grow the window on ACKs that repair an active loss episode.
        if acked and not was_recovering and self._recovery is None:
            self.cc.on_ack(len(acked))
        self._pump(now)

    # ------------------------------------------------------------------ gửi gói
    def _inflight_total(self) -> int:
        return sum(s.inflight for s in self.send_streams.values())

    def _pump(self, now: float | None = None) -> None:
        """Gửi các gói đang chờ, chừng nào cwnd còn cho phép. Round-robin giữa các stream để công bằng."""
        if now is None:
            now = self._clock()
        self._pump_retransmit(now)
        progress = True
        while progress and not self.closed:
            progress = False
            for stream in list(self.send_streams.values()):
                if stream.can_send() and self._inflight_total() < self.cc.allowed_inflight():
                    pkt = stream.pending.popleft()
                    wire = self._encode_packet(pkt)
                    stream.mark_sent(pkt, now, self.rtt.rto, wire)
                    self._send_wire(wire, now)
                    self.stats["sent"] += 1
                    self.stats["sent_payload_bytes"] += len(pkt.payload)
                    progress = True

    def _encode_packet(self, pkt: Packet) -> bytes:
        pkt.conn_id = self.conn_id
        if pkt.ptype == C.PKT_DATA:                 # chỉ mã hóa payload của gói DATA
            aad = pkt.header_bytes(len(pkt.payload) + C.GCM_TAG_SIZE)
            return aad + self.cipher.encrypt(pkt.stream_id, pkt.seq, aad, pkt.payload)
        return encode(pkt)

    def _send_wire(self, data: bytes, now: float) -> None:
        self.last_send = now
        self._sendto(data, self.remote_addr)

    def _transmit(self, pkt: Packet) -> None:
        self._send_wire(self._encode_packet(pkt), self._clock())

    def _queue_retransmit(self, stream_id: int, seq: int, reason: str) -> None:
        key = (stream_id, seq)
        if key not in self._retransmit_pending or reason == "timeout":
            self._retransmit_pending[key] = reason

    def _enter_recovery(self, *, timeout: bool) -> None:
        if self._recovery is None:
            self._recovery = {sid: stream.highest_sent for sid, stream in self.send_streams.items()
                              if stream.unacked}
            self._recovery_timeout = False
            self.stats["recovery_epochs"] += 1
            if not timeout:
                self.cc.on_loss()
                self.stats["loss_reductions"] += 1
        # Fast recovery can escalate once to RTO recovery, never per packet.
        if timeout and not self._recovery_timeout:
            self.cc.on_timeout()
            self._recovery_timeout = True
            self.stats["timeout_reductions"] += 1

    def _maybe_end_recovery(self) -> None:
        if self._recovery is not None and not any(
                seq <= boundary for sid, boundary in self._recovery.items()
                for seq in self.send_streams[sid].unacked):
            self._recovery = None
            self._recovery_timeout = False

    def _pump_retransmit(self, now: float) -> None:
        # One shared pacing slot for both fast and timeout retransmits. It is
        # deliberately independent of inflight<cwnd, which may be false after
        # a loss reduction; using that condition here would deadlock recovery.
        while self._retransmit_pending and not self.closed:
            key = next(iter(self._retransmit_pending))
            sid, seq = key
            inf = self.send_streams[sid].unacked.get(seq)
            if inf is None:
                self._retransmit_pending.pop(key)
                continue
            if now < self._next_retransmit_at:
                return
            reason = self._retransmit_pending.pop(key)
            if inf.retries >= C.MAX_RETRIES:
                self._finish("too many retries")
                return
            self.send_streams[sid].mark_retransmitted(seq, now, timeout=reason == "timeout")
            self._next_retransmit_at = now + C.TICK_INTERVAL
            self.stats["retransmitted"] += 1
            self.stats[reason + "_retransmitted"] += 1
            log.debug("retransmit conn=%s stream=%s seq=%s reason=%s retry=%s cwnd=%.2f",
                      self.conn_id, sid, seq, reason, inf.retries, self.cc.cwnd)
            self._send_wire(inf.wire_data, now)
            return

    # ------------------------------------------------------------------ timer
    def on_tick(self, now: float) -> None:
        """Endpoint gọi mỗi TICK_INTERVAL giây."""
        if self.closed:
            return
        if now - self.last_recv > C.PEER_TIMEOUT:
            self._finish("timeout")
            return
        if now - self.last_send > C.HEARTBEAT_INTERVAL:
            self._transmit(Packet(C.PKT_PING))

        timed_out_any = False
        for stream in list(self.send_streams.values()):
            for inf in stream.timed_out(now):
                if inf.retries >= C.MAX_RETRIES:
                    self._finish("too many retries")            # Bước 7: coi như mất kết nối
                    return
                key = (stream.stream_id, inf.pkt.seq)
                if self._retransmit_pending.get(key) != "timeout":
                    self._queue_retransmit(*key, "timeout")
                    timed_out_any = True
        if timed_out_any:
            self.stats["timeout_count"] += 1
            self._enter_recovery(timeout=True)
        self._pump(now)

    def _finish(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        self._retransmit_pending.clear()
        self._recovery = None
        for stream in self.send_streams.values():
            stream.pending.clear()
            stream.unacked.clear()
        self.send_streams.clear()
        self.recv_streams.clear()
        log.info("conn %s đóng: %s", self.conn_id, reason)
        self._on_closed(self.conn_id, reason)
