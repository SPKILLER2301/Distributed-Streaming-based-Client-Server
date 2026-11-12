"""
Ước lượng RTT/RTO và điều khiển tắc nghẽn.                [Người phụ trách: A]  (Bước 6, 7)

  RttEstimator          : SRTT/RTTVAR/RTO theo RFC 6298 (đã cài sẵn).
  CongestionController  : AIMD baseline (tên cũ được giữ để tương thích).
  CubicController       : CUBIC theo RFC 9438, thích nghi cho cửa sổ packet.
  A3: Connection quản lý recovery, chỉ gọi loss/timeout một lần mỗi đợt
      (cho phép loss -> timeout escalation). Backoff nằm ở packet, không nhân RTO chung.

Connection gọi:  cc.on_ack(n_acked)  khi nhận ACK mới;  cc.on_loss()  khi phát hiện mất gói (SACK/fast retransmit);
                 cc.on_timeout() khi RTO hết hạn.
"""
from __future__ import annotations

import math
import time

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
        """Legacy helper; A3 uses per-packet backoff instead of calling this."""
        self.rto = min(self.rto * 2, C.MAX_RTO)


class CongestionController:
    """Original AIMD baseline, including its original ACK/loss behavior."""
    algorithm = "aimd"

    def __init__(self) -> None:
        self.cwnd: float = float(C.INITIAL_CWND)     # đơn vị: gói
        self.ssthresh: float = float(C.MAX_WINDOW)

    def on_ack(self, n_acked: int, *, now=None, srtt=None, cwnd_limited=True) -> None:
        for _ in range(n_acked):
            if self.cwnd < self.ssthresh:
                self.cwnd += 1.0                     # slow start: +1 mỗi ACK (gấp đôi mỗi RTT)
            else:
                self.cwnd += 1.0 / self.cwnd         # congestion avoidance: ~ +1 mỗi RTT
        self.cwnd = min(self.cwnd, float(C.MAX_WINDOW))

    def on_loss(self, *, now=None, flight_size=None) -> None:
        """Mất gói phát hiện qua SACK/dup ACK: giảm một nửa."""
        self.ssthresh = max(self.cwnd / 2, 2.0)
        self.cwnd = self.ssthresh

    def on_timeout(self, *, now=None, flight_size=None) -> None:
        """RTO hết hạn: nặng hơn, quay về cwnd nhỏ."""
        self.ssthresh = max(self.cwnd / 2, 2.0)
        self.cwnd = 1.0

    def allowed_inflight(self) -> int:
        return max(1, int(self.cwnd))

    def set_application_limited(self, limited: bool, *, now=None) -> None:
        """AIMD has no time-based epoch to pause."""


class CubicController(CongestionController):
    """RFC 9438 sections 4.2--4.8; window unit is DATA packets, not bytes.

    Each newly ACKed packet counts once, including short/empty packets. This
    deliberately matches the transport's packet budget; it is not SMSS-byte
    accounting or a claim of TCP wire/fairness compliance. Reno slow start is
    retained for this bounded educational transport (no HyStart++/ECN/undo).
    Connection owns loss recovery and tells us when a flight filled cwnd.
    """
    algorithm = "cubic"
    CUBIC_C = 0.4
    BETA = 0.7
    ALPHA = 3 * (1 - BETA) / (1 + BETA)

    def __init__(self, *, clock=time.monotonic, fast_convergence=True) -> None:
        super().__init__()
        self._clock = clock
        self.fast_convergence = fast_convergence
        self.w_max = self.cwnd
        self.cwnd_prior = self.cwnd
        self.cwnd_epoch = self.cwnd
        self.w_est = self.cwnd
        self.k = 0.0
        self.epoch_start: float | None = None
        self.alpha = self.ALPHA
        self._restart = True  # First avoidance stage and first stage after RTO.
        self._first_epoch = True
        self._limited_since: float | None = self._clock()

    def set_application_limited(self, limited: bool, *, now=None) -> None:
        now = self._clock() if now is None else now
        if limited and self._limited_since is None:
            self._limited_since = now
        elif not limited and self._limited_since is not None:
            if self.epoch_start is not None:
                self.epoch_start += max(0.0, now - self._limited_since)
            self._limited_since = None

    def _start_epoch(self, now: float) -> None:
        self.epoch_start = now
        self.cwnd_epoch = self.cwnd
        if self._first_epoch:
            self.cwnd_prior = self.cwnd
            self._first_epoch = False
        if self._restart:
            self.w_max = self.cwnd
            self.k = 0.0
            self._restart = False
        else:
            # Fast convergence can put w_max below the new cwnd. A signed
            # cube root preserves W_cubic(0) == cwnd_epoch in that case.
            self.k = math.cbrt((self.w_max - self.cwnd_epoch) / self.CUBIC_C)
        self.w_est = self.cwnd
        self.alpha = self.ALPHA

    def on_ack(self, n_acked: int, *, now=None, srtt=None, cwnd_limited=True) -> None:
        if n_acked <= 0:
            return
        now = self._clock() if now is None else now
        if not cwnd_limited:
            self.set_application_limited(True, now=now)
            return
        self.set_application_limited(False, now=now)
        # Missing RTT contributes no lookahead. Do not substitute RTO for
        # a Karn-safe SRTT sample in the cubic target calculation.
        rtt = max(0.0, srtt or 0.0)
        for _ in range(n_acked):
            if self.cwnd < self.ssthresh:
                self.cwnd = min(self.cwnd + 1.0, self.ssthresh)
            else:
                if self.epoch_start is None:
                    self._start_epoch(now)
                elapsed = max(0.0, now - self.epoch_start)
                cubic_now = self.CUBIC_C * (elapsed - self.k) ** 3 + self.w_max
                target = self.CUBIC_C * (elapsed + rtt - self.k) ** 3 + self.w_max
                target = min(max(target, self.cwnd), 1.5 * self.cwnd)
                self.w_est += self.alpha / self.cwnd
                if self.w_est >= self.cwnd_prior:
                    self.alpha = 1.0
                if cubic_now < self.w_est:
                    self.cwnd = max(self.cwnd, self.w_est)
                else:
                    self.cwnd += (target - self.cwnd) / self.cwnd
            self.cwnd = min(max(self.cwnd, 1.0), float(C.MAX_WINDOW))
            self.w_est = min(self.w_est, float(C.MAX_WINDOW))

    def _reduce(self, flight_size, now, *, timeout):
        now = self._clock() if now is None else now
        prior = self.cwnd
        if self.fast_convergence and prior < self.w_max:
            self.w_max = prior * (1 + self.BETA) / 2
        else:
            self.w_max = prior
        self.cwnd_prior = prior
        self._first_epoch = False
        # Use the pre-ACK outstanding flight, bounded by the current window.
        # On a fast-loss -> timeout escalation do not raise cwnd/ssthresh
        # merely because the older, larger flight is still outstanding.
        flight = prior if flight_size is None else min(prior, max(0, flight_size))
        self.ssthresh = max(flight * self.BETA, 2.0)
        self.cwnd = 1.0 if timeout else self.ssthresh
        self.epoch_start = None
        self._restart = timeout
        self.set_application_limited(True, now=now)

    def on_loss(self, *, now=None, flight_size=None) -> None:
        self._reduce(flight_size, now, timeout=False)

    def on_timeout(self, *, now=None, flight_size=None) -> None:
        self._reduce(flight_size, now, timeout=True)


def make_controller(algorithm: str = "cubic", *, clock=time.monotonic):
    """Validate before opening sockets; each connection gets its own state."""
    if algorithm == "cubic":
        return CubicController(clock=clock)
    if algorithm == "aimd":
        return CongestionController()
    raise ValueError("congestion_algorithm must be 'cubic' or 'aimd'")
