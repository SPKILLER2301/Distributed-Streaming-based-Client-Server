"""
Mô phỏng mạng xấu: mất gói, trễ, đảo thứ tự, nhân đôi gói.   [Người phụ trách: A]  (Bước 10)

Cách dùng trong test:
    ep.link = LossyLink(loss=0.1, delay=0.01, jitter=0.02, duplicate=0.02, seed=1)
Endpoint.sendto() sẽ gọi link.send(...) thay vì gửi thẳng. Gắn vào CẢ HAI endpoint để mô phỏng 2 chiều.
Jitter > 0 làm các gói đến không đúng thứ tự (đảo thứ tự).
Có thể dùng `tc netem` (Linux) để kiểm thử thêm, nhưng LossyLink chạy được trên mọi hệ điều hành.
"""
import asyncio
import random
from common.packet import decode, PacketError


class LossyLink:
    def __init__(self, loss: float = 0.0, delay: float = 0.0, jitter: float = 0.0,
                 duplicate: float = 0.0, seed: int | None = None, *,
                 drop_rule=None, record: bool = False) -> None:
        self.loss, self.delay, self.jitter, self.duplicate = loss, delay, jitter, duplicate
        self._rng = random.Random(seed)
        self.sent = self.dropped = 0
        # Optional deterministic hook: drop_rule(packet, attempt_for_identity).
        # Recording is opt-in and intended for finite test runs only.
        self.drop_rule, self.record = drop_rule, record
        self.events = []
        self._attempts = {}
        self._handles = set()

    def send(self, real_sendto, data: bytes, addr) -> None:
        self.sent += 1
        pkt = None
        attempt = 0
        if self.drop_rule is not None or self.record:
            try:
                pkt = decode(data)
            except PacketError:
                pass
            if pkt is not None:
                key = (pkt.conn_id, pkt.ptype, pkt.stream_id, pkt.seq, pkt.ack, pkt.sack)
                attempt = self._attempts.get(key, 0) + 1
                self._attempts[key] = attempt
        targeted = pkt is not None and self.drop_rule is not None and self.drop_rule(pkt, attempt)
        dropped = targeted or self._rng.random() < self.loss
        if self.record:
            self.events.append({"time": asyncio.get_running_loop().time(),
                                "packet": pkt, "attempt": attempt,
                                "dropped": bool(dropped), "wire": data})
        if dropped:
            self.dropped += 1
            return
        copies = 2 if self._rng.random() < self.duplicate else 1
        for _ in range(copies):
            wait = self.delay + self._rng.uniform(0, self.jitter)
            if wait <= 0:
                real_sendto(data, addr)
            else:
                handle = None

                def deliver(handle_ref, payload=data, destination=addr):
                    self._handles.discard(handle_ref[0])
                    real_sendto(payload, destination)

                handle_ref = [None]
                handle = asyncio.get_running_loop().call_later(wait, deliver, handle_ref)
                handle_ref[0] = handle
                self._handles.add(handle)

    def close(self) -> None:
        """Cancel delayed deliveries before the Endpoint's socket is closed."""
        for handle in self._handles:
            handle.cancel()
        self._handles.clear()
