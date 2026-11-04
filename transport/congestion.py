"""
Ước lượng RTT/RTO và điều khiển tắc nghẽn.                [Người phụ trách: A]  (Bước 6, 7)

  RttEstimator          : SRTT/RTTVAR/RTO theo RFC 6298 (đã cài sẵn).
  CongestionController  : AIMD đơn giản (slow start + congestion avoidance).
                          Đề nhắc tới BBR/CUBIC: nếu còn thời gian, thay thân hàm on_ack/on_loss
                          bằng CUBIC (cwnd = C*(t-K)^3 + Wmax) và nêu rõ trong báo cáo. AIMD đủ để có điểm.

Connection gọi:  cc.on_ack(n_acked)  khi nhận ACK mới;  cc.on_loss()  khi phát hiện mất gói (SACK/fast retransmit);
                 cc.on_timeout() khi RTO hết hạn.
"""
from __future__ import annotations

from common import constants as C


class RttEstimator:
    def __init__(self) -> None:
        self.srtt: float | None = None
        self.rttvar: float = 0.0
        self.rto: float = C.INITIAL_RTO

    def update(self, sample: float) -> None:
        """Gọi với RTT đo được. CHỈ lấy mẫu từ gói KHÔNG bị gửi lại (thuật toán Karn)."""
        if self.srtt is None:
            self.srtt, self.rttvar = sample, sample / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - sample)
            self.srtt = 0.875 * self.srtt + 0.125 * sample
        self.rto = min(max(self.srtt + 4 * self.rttvar, C.MIN_RTO), C.MAX_RTO)

    def backoff(self) -> None:
        """Hết RTO: nhân đôi RTO (exponential backoff)."""
        self.rto = min(self.rto * 2, C.MAX_RTO)


class CongestionController:
    def __init__(self) -> None:
        self.cwnd: float = float(C.INITIAL_CWND)     # đơn vị: gói
        self.ssthresh: float = float(C.MAX_WINDOW)

    def on_ack(self, n_acked: int) -> None:
        for _ in range(n_acked):
            if self.cwnd < self.ssthresh:
                self.cwnd += 1.0                     # slow start: +1 mỗi ACK (gấp đôi mỗi RTT)
            else:
                self.cwnd += 1.0 / self.cwnd         # congestion avoidance: ~ +1 mỗi RTT
        self.cwnd = min(self.cwnd, float(C.MAX_WINDOW))

    def on_loss(self) -> None:
        """Mất gói phát hiện qua SACK/dup ACK: giảm một nửa."""
        self.ssthresh = max(self.cwnd / 2, 2.0)
        self.cwnd = self.ssthresh

    def on_timeout(self) -> None:
        """RTO hết hạn: nặng hơn, quay về cwnd nhỏ."""
        self.ssthresh = max(self.cwnd / 2, 2.0)
        self.cwnd = 1.0

    def allowed_inflight(self) -> int:
        return max(1, int(self.cwnd))
