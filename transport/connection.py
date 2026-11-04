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

CÒN THIẾU – TODO(A) (xem on_tick và _on_ack):
  1. Fast retransmit khi SACK cho thấy có "lỗ hổng" (không phải chờ hết RTO)
  2. Gửi lại có giới hạn theo cwnd, backoff theo từng gói
  3. (tùy chọn) CUBIC; thống kê RTT/cwnd/gói gửi lại để hiển thị lên UI
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Dict, Tuple

from common import constants as C
from common.packet import Packet, encode

from .congestion import CongestionController, RttEstimator
from .crypto import CryptoError, SessionCipher
from .stream import RecvStream, SendStream

log = logging.getLogger(__name__)
Addr = Tuple[str, int]


class Connection:
    def __init__(self, conn_id: int, remote_addr: Addr, cipher: SessionCipher,
                 sendto: Callable[[bytes, Addr], None],
                 on_message: Callable[[int, int, bytes], None],
                 on_closed: Callable[[int, str], None]) -> None:
        self.conn_id = conn_id
        self.remote_addr = remote_addr
        self.cipher = cipher
        self._sendto = sendto
        self._on_message = on_message
        self._on_closed = on_closed

        self.send_streams: Dict[int, SendStream] = {}
        self.recv_streams: Dict[int, RecvStream] = {}
        self.rtt = RttEstimator()
        self.cc = CongestionController()
        self.closed = False
        now = time.monotonic()
        self.last_recv = now
        self.last_send = now
        self._next_stream_id = C.STREAM_FILE_BASE
        self.stats = {"sent": 0, "retransmitted": 0}

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
        stream.enqueue(data)
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
        self.last_recv = time.monotonic()
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
        now = time.monotonic()
        acked = stream.on_ack(pkt.ack, pkt.sack)
        for inf in acked:
            if inf.retries == 0:                    # Karn: chỉ đo RTT từ gói chưa gửi lại
                self.rtt.update(now - inf.sent_at)
        if acked:
            self.cc.on_ack(len(acked))
        self._pump()

    # ------------------------------------------------------------------ gửi gói
    def _inflight_total(self) -> int:
        return sum(s.inflight for s in self.send_streams.values())

    def _pump(self) -> None:
        """Gửi các gói đang chờ, chừng nào cwnd còn cho phép. Round-robin giữa các stream để công bằng."""
        now = time.monotonic()
        progress = True
        while progress and not self.closed:
            progress = False
            for stream in list(self.send_streams.values()):
                if stream.pending and self._inflight_total() < self.cc.allowed_inflight():
                    pkt = stream.pending.popleft()
                    stream.mark_sent(pkt, now)
                    self._transmit(pkt)
                    self.stats["sent"] += 1
                    progress = True

    def _transmit(self, pkt: Packet) -> None:
        pkt.conn_id = self.conn_id
        if pkt.ptype == C.PKT_DATA:                 # chỉ mã hóa payload của gói DATA
            aad = pkt.header_bytes(len(pkt.payload) + C.GCM_TAG_SIZE)
            data = aad + self.cipher.encrypt(pkt.stream_id, pkt.seq, aad, pkt.payload)
        else:
            data = encode(pkt)
        self.last_send = time.monotonic()
        self._sendto(data, self.remote_addr)

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

        # Gửi lại cơ bản theo timeout (BASELINE – A cải tiến thêm).
        timed_out_any = False
        for stream in list(self.send_streams.values()):
            for inf in stream.timed_out(now, self.rtt.rto):
                if inf.retries >= C.MAX_RETRIES:
                    self._finish("too many retries")            # Bước 7: coi như mất kết nối
                    return
                self._transmit(inf.pkt)
                stream.mark_retransmitted(inf.pkt.seq, now)
                self.stats["retransmitted"] += 1
                timed_out_any = True
        if timed_out_any:
            self.rtt.backoff()                                  # RTO nhân đôi
            self.cc.on_timeout()                                # cwnd về nhỏ
        # TODO(A): cải tiến gửi lại:
        #   - FAST RETRANSMIT: trong _on_ack, nếu SACK cho thấy các seq phía sau ack đã tới mà seq == ack
        #     vẫn thiếu (≈3 ACK trùng) thì gửi lại NGAY và gọi self.cc.on_loss() (chỉ giảm nửa cwnd).
        #   - Hiện mọi gói quá hạn bị gửi lại cùng lúc (có thể thành đợt burst) -> giới hạn theo cwnd.
        #   - Backoff theo từng gói thay vì dùng chung một RTO cho cả kết nối.
        #   - Phát hiện gói gửi lại bị coi là trùng ở bên nhận (đã xử lý: RecvStream bỏ gói trùng).
        self._pump()

    def _finish(self, reason: str) -> None:
        if self.closed:
            return
        self.closed = True
        log.info("conn %s đóng: %s", self.conn_id, reason)
        self._on_closed(self.conn_id, reason)
