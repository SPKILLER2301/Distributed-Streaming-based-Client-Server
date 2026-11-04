"""
Xác thực đăng nhập.                                       [Người phụ trách: B]  (Bước 3)

Đọc server/data/users.json: {"username": "sha256(password) dạng hex"}.
Mật khẩu đi qua mạng đã được mã hóa AES-GCM ở tầng transport.

TODO(B) (tùy chọn): thêm salt (PBKDF2/scrypt) thay vì sha256 trần; giới hạn số lần đăng nhập sai.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class Auth:
    def __init__(self, users_file: str | Path) -> None:
        try:
            self._users = json.loads(Path(users_file).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            log.error("không đọc được %s – sẽ từ chối mọi đăng nhập", users_file)
            self._users = {}

    def verify(self, username: str, password: str) -> bool:
        expected = self._users.get(username)
        if expected is None:
            return False
        actual = hashlib.sha256(password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(expected, actual)      # so sánh hằng thời gian
