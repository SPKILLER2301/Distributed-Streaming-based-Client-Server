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
A5 adds bounded queues/byte+packet credit and priority scheduling on wire v2.
A4 defaults to CUBIC; AIMD remains selectable as a comparison baseline.
"""
from __future__ import annotations

import logging
import time
import struct
from collections import OrderedDict
from typing import Callable, Dict, Tuple

from common import constants as C
from common.packet import Packet, encode

from .congestion import make_controller, RttEstimator
from .crypto import CryptoError, SessionCipher
from .stream import InvalidAck, RecvStream, SendStream
from .flow import FlowLimits, ConnectionClosed, MessageTooLarge, StreamLimitError
from .flow_control import FlowControl, CREDIT, RESET

log = logging.getLogger(__name__)
Addr = Tuple[str, int]


class Connection(FlowControl):
    def __init__(self, conn_id: int, remote_addr: Addr, cipher: SessionCipher,
                 sendto: Callable[[bytes, Addr], None],
                 on_message: Callable[[int, int, bytes], None],
                 on_closed: Callable[[int, str], None], *,
                 clock: Callable[[], float] = time.monotonic,
                 flow_limits: FlowLimits | None = None,
                 on_delivery=None, on_stream_reset=None,
                 congestion_algorithm: str = "cubic") -> None:
        self.conn_id = conn_id
        self.remote_addr = remote_addr
        self.cipher = cipher
        self._sendto = sendto
        self._on_message = on_message
        self._on_closed = on_closed
        self._clock = clock
        self._init_flow(flow_limits or FlowLimits(), on_delivery, on_stream_reset)

        self.send_streams: Dict[int, SendStream] = {}
        self.recv_streams: Dict[int, RecvStream] = {}
        self.rtt = RttEstimator()
        self.cc = make_controller(congestion_algorithm, clock=clock)
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
        self.stats["flow_blocked"] = 0
        self._retransmit_pending: OrderedDict[tuple[int, int], str] = OrderedDict()
        self._next_retransmit_at = now
        self._recovery: Dict[int, int] | None = None
        self._recovery_timeout = False
        self._control_pending = OrderedDict()
        self._control_at = now
        self._control_used = 0
        self._control_counter = 0
        self._peer_control_counter = -1
        self._peer_control_bitmap = 0
        log.info("conn %s congestion=%s initial_cwnd=%.2f", conn_id,
                 self.cc.algorithm, self.cc.cwnd)

    # ------------------------------------------------------------------ API cho Endpoint
    def open_stream(self) -> int:
        """Cấp stream_id mới cho chiều gửi của mình (dùng cho mỗi file)."""
        sid = self._next_stream_id
        while sid in self.send_streams:
            sid += 1
        self._send_stream(sid)
        self._next_stream_id = sid + 1
        return sid

    def send_message(self, stream_id: int, data: bytes):
        return self._enqueue(stream_id, data)

    def close(self, reason: str = "closed locally") -> None:
        if self.closed:
            return
        self._transmit(Packet(C.PKT_CLOSE, payload=b"\x00"))
        self._finish(reason)

    # ------------------------------------------------------------------ nhận gói
    def handle_packet(self, pkt: Packet) -> None:
        if self.closed:
            return
        if pkt.ptype == C.PKT_DATA:
            self._on_data(pkt)
            return
        sizes = {C.PKT_ACK: 0, C.PKT_PING: 0, C.PKT_PONG: 0, C.PKT_CLOSE: 1,
                 C.PKT_CREDIT: CREDIT.size, C.PKT_CREDIT_PROBE: 0,
                 C.PKT_RESET_STREAM: RESET.size, C.PKT_RESET_ACK: RESET.size,
                 C.PKT_STOP_STREAM: 1}
        if pkt.ptype not in sizes or not self._authenticate_control(pkt):
            return
        if (pkt.flags or len(pkt.payload) != sizes[pkt.ptype] or pkt.seq or
                (pkt.ptype != C.PKT_ACK and (pkt.ack or pkt.sack))):
            return
        try:
            if pkt.ptype == C.PKT_ACK:
                if not self._on_ack(pkt):
                    return
            elif pkt.ptype == C.PKT_PING:
                self._transmit(Packet(C.PKT_PONG))
            elif pkt.ptype == C.PKT_CLOSE:
                self._finish("closed by peer")
            elif pkt.ptype == C.PKT_CREDIT:
                self._on_credit(pkt)
            elif pkt.ptype == C.PKT_CREDIT_PROBE:
                self._recv_stream(pkt.stream_id)
                self._advertise_credit(pkt.stream_id)
            elif pkt.ptype == C.PKT_RESET_STREAM:
                self._on_reset(pkt)
            elif pkt.ptype == C.PKT_RESET_ACK:
                self._on_reset_ack(pkt)
            elif pkt.ptype == C.PKT_STOP_STREAM:
                self.begin_cancel(pkt.stream_id)
            self.last_recv = self._clock()
        except (ValueError, StreamLimitError):
            self.close("protocol_error")

    def _on_data(self, pkt: Packet) -> None:
        if pkt.flags & ~C.FLAG_EOM or len(pkt.payload) > C.MAX_PAYLOAD + C.GCM_TAG_SIZE:
            return
        aad = pkt.header_bytes()                    # header dựng lại giống hệt bên gửi
        try:
            plain = self.cipher.decrypt(pkt.stream_id, pkt.seq, aad, pkt.payload)
        except CryptoError:
            log.debug("bỏ gói không giải mã được (conn %s)", self.conn_id)
            return
        try:
            self._receive_data(pkt, plain)
            self.last_recv = self._clock()
        except (MessageTooLarge, StreamLimitError, ValueError):
            self.close("protocol_error")
        except Exception:
            log.exception("receive callback failed")
            self.close("receive handler error")

    def _on_ack(self, pkt: Packet) -> bool:
        stream = self.send_streams.get(pkt.stream_id)
        if stream is None or stream.cancelled:
            return False
        now = self._clock()
        flight_before_ack = self._inflight_total()
        was_recovering = self._recovery is not None
        try:
            acked = stream.on_ack(pkt.ack, pkt.sack)
        except InvalidAck:
            self.stats["invalid_acks"] += 1
            return False
        for inf in acked:
            self._ack_payload(stream, inf)
            self._retransmit_pending.pop((pkt.stream_id, inf.pkt.seq), None)
            if inf.retries == 0:                    # Karn: chỉ đo RTT từ gói chưa gửi lại
                self.rtt.update(now - inf.sent_at)
        self.stats["acked_packets"] += len(acked)
        self.stats["acked_payload_bytes"] += sum(len(inf.pkt.payload) for inf in acked)
        self._maybe_end_recovery()
        for inf in stream.fast_retransmit_candidates():
            inf.fast_retransmitted = True
            self._queue_retransmit(pkt.stream_id, inf.pkt.seq, "fast")
            self._enter_recovery(timeout=False, flight_size=flight_before_ack)
        # Do not grow the window on ACKs that repair an active loss episode.
        if acked and not was_recovering and self._recovery is None:
            eligible = sum(inf.cwnd_limited for inf in acked)
            # Keep the original AIMD baseline's behavior. CUBIC grows only
            # for ACKs from a flight that actually filled its packet window.
            self.cc.on_ack(len(acked) if self.cc.algorithm == "aimd" else eligible,
                           now=now, srtt=self.rtt.srtt)
        log.debug("cc t=%.6f conn=%s algorithm=%s cwnd=%.4f inflight=%s acked=%s recovery=%s",
                  now, self.conn_id, self.cc.algorithm, self.cc.cwnd,
                  self._inflight_total(), len(acked), self._recovery is not None)
        self._pump(now)
        return True

    # ------------------------------------------------------------------ gửi gói
    def _inflight_total(self) -> int:
        return sum(s.inflight for s in self.send_streams.values())

    def _sync_cc_usage(self, now: float) -> None:
        outstanding = [inf for s in self.send_streams.values() for inf in s.unacked.values()]
        budget = self.cc.allowed_inflight()
        if budget > 0 and len(outstanding) >= budget:
            for inf in outstanding:
                inf.cwnd_limited = True
        self.cc.set_application_limited(not any(inf.cwnd_limited for inf in outstanding), now=now)

    def _pump(self, now: float | None = None) -> None:
        """Gửi các gói đang chờ, chừng nào cwnd còn cho phép. Round-robin giữa các stream để công bằng."""
        if now is None:
            now = self._clock()
        self._pump_retransmit(now)
        while not self.closed and self._inflight_total() < self.cc.allowed_inflight():
            stream = self._select_stream()
            if stream is None:
                break
            pkt = stream.pending.popleft()
            wire = self._encode_packet(pkt)
            stream.mark_sent(pkt, now, self.rtt.rto, wire)
            self._record_sent(stream, pkt)
            self._send_wire(wire, now)
            self.stats["sent"] += 1
            self.stats["sent_payload_bytes"] += len(pkt.payload)
        self._sync_cc_usage(now)
        self._flush_control(now)

    def _encode_packet(self, pkt: Packet) -> bytes:
        pkt.conn_id = self.conn_id
        if pkt.ptype == C.PKT_DATA:                 # chỉ mã hóa payload của gói DATA
            aad = pkt.header_bytes(len(pkt.payload) + C.GCM_TAG_SIZE)
            return aad + self.cipher.encrypt(pkt.stream_id, pkt.seq, aad, pkt.payload)
        if self._control_counter >= 1 << 64:
            self._finish("control counter exhausted")
            raise ConnectionClosed("control counter exhausted")
        counter = struct.pack("!Q", self._control_counter)
        self._control_counter += 1
        header = pkt.header_bytes(len(pkt.payload) + 24)
        return header + counter + self.cipher.encrypt_control(
            self._control_counter - 1, header + counter, pkt.payload)

    def _authenticate_control(self, pkt):
        if len(pkt.payload) < 24 or len(pkt.payload) + C.HEADER_SIZE > C.MAX_DATAGRAM:
            return False
        counter_bytes = pkt.payload[:8]
        counter = struct.unpack("!Q", counter_bytes)[0]
        distance = self._peer_control_counter - counter
        if distance >= 256 or (distance >= 0 and (self._peer_control_bitmap >> distance) & 1):
            return False
        try:
            body = self.cipher.decrypt_control(counter, pkt.header_bytes() + counter_bytes, pkt.payload[8:])
        except CryptoError:
            return False
        if counter > self._peer_control_counter:
            shift = counter - self._peer_control_counter
            self._peer_control_bitmap = ((self._peer_control_bitmap << min(256, shift)) | 1) & ((1 << 256) - 1)
            self._peer_control_counter = counter
        else:
            self._peer_control_bitmap |= 1 << distance
        pkt.payload = body
        return True

    def _send_wire(self, data: bytes, now: float) -> None:
        self.last_send = now
        self._sendto(data, self.remote_addr)

    def _transmit(self, pkt: Packet) -> None:
        # Control traffic is independent of DATA credit and coalesced so a
        # blocked consumer cannot cause an unbounded queue of ACK/credit frames.
        self._control_pending[pkt.ptype, pkt.stream_id] = pkt
        self._flush_control(self._clock())

    def _flush_control(self, now):
        if now >= self._control_at + C.TICK_INTERVAL:
            self._control_at, self._control_used = now, 0
        while self._control_pending and self._control_used < C.CONTROL_BUDGET and not self.closed:
            _, pkt = self._control_pending.popitem(last=False)
            self._control_used += 1
            self._send_wire(self._encode_packet(pkt), now)

    def _queue_retransmit(self, stream_id: int, seq: int, reason: str) -> None:
        key = (stream_id, seq)
        if key not in self._retransmit_pending or reason == "timeout":
            self._retransmit_pending[key] = reason

    def _enter_recovery(self, *, timeout: bool, flight_size=None) -> None:
        now = self._clock()
        if flight_size is None:
            flight_size = self._inflight_total()
        if self._recovery is None:
            self._recovery = {sid: stream.highest_sent for sid, stream in self.send_streams.items()
                              if stream.unacked}
            self._recovery_timeout = False
            self.stats["recovery_epochs"] += 1
            if not timeout:
                self.cc.on_loss(now=now, flight_size=flight_size)
                self.stats["loss_reductions"] += 1
        # Fast recovery can escalate once to RTO recovery, never per packet.
        if timeout and not self._recovery_timeout:
            self.cc.on_timeout(now=now, flight_size=flight_size)
            self._recovery_timeout = True
            self.stats["timeout_reductions"] += 1
        log.debug("cc t=%.6f conn=%s algorithm=%s event=%s cwnd=%.4f inflight=%s",
                  now, self.conn_id, self.cc.algorithm, "timeout" if timeout else "loss",
                  self.cc.cwnd, flight_size)

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
        self._flow_tick(now)
        if self.closed:
            return

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
        self._close_flow(reason)
        self._control_pending.clear()
        self._retransmit_pending.clear()
        self._recovery = None
        for stream in self.send_streams.values():
            stream.pending.clear()
            stream.unacked.clear()
        self.send_streams.clear()
        self.recv_streams.clear()
        log.info("conn %s đóng: %s", self.conn_id, reason)
        self._on_closed(self.conn_id, reason)
