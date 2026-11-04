"""
Chia chunk, SHA-256, ghép file.                           [Người phụ trách: A]  (Bước 6)

Module thuần (không biết mạng) nên test được riêng (tests/test_file_transfer.py).

  FileSender   : đọc file, tính SHA-256, sinh ra các chunk FILE_CHUNK_SIZE byte.
  FileReceiver : ghi chunk xuống đĩa, đếm byte, kiểm SHA-256 khi đủ.

Việc đảm bảo "không hỏng dữ liệu" gồm 2 lớp: AES-GCM + ACK/gửi lại ở transport (từng gói),
và SHA-256 toàn file ở đây (phát hiện lỗi ghép/ghi).

TODO(A): (1) tiến trình gửi hiện đếm theo chunk ĐÃ ĐẨY VÀO transport, chưa phải đã được ACK
             -> nếu muốn chính xác, thêm callback "đã ACK" từ transport lên.
         (2) Gửi file lớn: đừng đẩy toàn bộ chunk vào transport một lúc (tốn RAM).
             Gợi ý: gửi theo lô ~64 chunk rồi `await asyncio.sleep(0.01)` / chờ hàng đợi vơi bớt.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import Iterator

from common import constants as C


class FileSender:
    def __init__(self, path: str, chunk_size: int = C.FILE_CHUNK_SIZE) -> None:
        self.path = path
        self.name = os.path.basename(path)
        self.size = os.path.getsize(path)
        self.chunk_size = chunk_size
        self.file_id = uuid.uuid4().hex[:8]
        self.sha256 = self._hash()
        self.sent = 0                                   # byte đã đẩy vào transport

    def _hash(self) -> str:
        h = hashlib.sha256()
        with open(self.path, "rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
        return h.hexdigest()

    def chunks(self) -> Iterator[bytes]:
        with open(self.path, "rb") as f:
            while True:
                data = f.read(self.chunk_size)
                if not data:
                    break
                self.sent += len(data)
                yield data


class FileReceiver:
    def __init__(self, file_id: str, name: str, size: int, sha256: str, dest_dir: str) -> None:
        self.file_id, self.size, self.expected_sha = file_id, size, sha256
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        self.path = self._unique_path(dest_dir, os.path.basename(name) or "file")   # chặn ../ trong tên
        self.received = 0
        self._hash = hashlib.sha256()
        self._fh = open(self.path, "wb")

    @staticmethod
    def _unique_path(dest_dir: str, name: str) -> str:
        base, ext = os.path.splitext(name)
        candidate, n = os.path.join(dest_dir, name), 1
        while os.path.exists(candidate):
            candidate = os.path.join(dest_dir, f"{base}({n}){ext}")
            n += 1
        return candidate

    def write(self, chunk: bytes) -> bool:
        """Ghi 1 chunk. Trả True khi đã nhận đủ `size` byte."""
        self._fh.write(chunk)
        self._hash.update(chunk)
        self.received += len(chunk)
        return self.received >= self.size

    def finish(self) -> bool:
        """Đóng file và kiểm SHA-256. Hỏng thì xóa file."""
        self._fh.close()
        ok = self.received == self.size and self._hash.hexdigest() == self.expected_sha
        if not ok:
            self.abort()
        return ok

    def abort(self) -> None:
        if not self._fh.closed:
            self._fh.close()
        try:
            os.remove(self.path)
        except OSError:
            pass

    @property
    def progress(self) -> float:
        return self.received / self.size if self.size else 1.0
