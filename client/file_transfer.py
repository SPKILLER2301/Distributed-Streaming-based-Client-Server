"""File chunks, exclusive output creation and SHA-256 verification. Owner: B."""

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
        self.file_id = uuid.uuid4().hex
        self.sha256 = self._hash()
        self.sent = 0  # byte đã đẩy vào transport

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
    def __init__(
        self, file_id: str, name: str, size: int, sha256: str, dest_dir: str
    ) -> None:
        if type(size) is not int or size < 0:
            raise ValueError("invalid file size")
        self.file_id, self.size, self.expected_sha = file_id, size, sha256
        Path(dest_dir).mkdir(parents=True, exist_ok=True)
        self.path = self._unique_path(
            dest_dir, os.path.basename(name.replace("\\", "/")) or "file"
        )  # chặn ../ trong tên
        self.received = 0
        self._hash = hashlib.sha256()
        while True:
            try:
                self._fh = open(self.path, "xb")
                break
            except FileExistsError:
                self.path = self._unique_path(
                    dest_dir, os.path.basename(name.replace("\\", "/")) or "file"
                )

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
        if self.received + len(chunk) > self.size:
            self.abort()
            raise ValueError("file exceeds offered size")
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
