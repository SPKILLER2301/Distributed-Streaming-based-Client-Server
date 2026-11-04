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


class LossyLink:
    def __init__(self, loss: float = 0.0, delay: float = 0.0, jitter: float = 0.0,
                 duplicate: float = 0.0, seed: int | None = None) -> None:
        self.loss, self.delay, self.jitter, self.duplicate = loss, delay, jitter, duplicate
        self._rng = random.Random(seed)
        self.sent = self.dropped = 0

    def send(self, real_sendto, data: bytes, addr) -> None:
        self.sent += 1
        if self._rng.random() < self.loss:
            self.dropped += 1
            return
        copies = 2 if self._rng.random() < self.duplicate else 1
        for _ in range(copies):
            wait = self.delay + self._rng.uniform(0, self.jitter)
            if wait <= 0:
                real_sendto(data, addr)
            else:
                asyncio.get_running_loop().call_later(wait, real_sendto, data, addr)
