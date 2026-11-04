"""
Danh sách user đang online.                               [Người phụ trách: B]  (Bước 3)

Server phải theo dõi: tên, conn_id, địa chỉ IP:port, thời điểm hoạt động cuối.
Chỉ là cấu trúc dữ liệu thuần – không gọi mạng. Dễ unit-test (tests/test_auth_routing.py).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class UserInfo:
    username: str
    conn_id: int
    addr: Tuple[str, int]
    last_seen: float = field(default_factory=time.time)


class Registry:
    def __init__(self) -> None:
        self._by_name: Dict[str, UserInfo] = {}
        self._by_conn: Dict[int, UserInfo] = {}

    def add(self, username: str, conn_id: int, addr: Tuple[str, int]) -> bool:
        """Trả False nếu tên đã đang online (không cho đăng nhập hai nơi)."""
        if username in self._by_name:
            return False
        info = UserInfo(username, conn_id, addr)
        self._by_name[username] = self._by_conn[conn_id] = info
        return True

    def remove_by_conn(self, conn_id: int) -> Optional[UserInfo]:
        info = self._by_conn.pop(conn_id, None)
        if info:
            self._by_name.pop(info.username, None)
        return info

    def get_by_name(self, username: str) -> Optional[UserInfo]:
        return self._by_name.get(username)

    def get_by_conn(self, conn_id: int) -> Optional[UserInfo]:
        return self._by_conn.get(conn_id)

    def usernames(self) -> List[str]:
        return sorted(self._by_name)

    def all(self) -> List[UserInfo]:
        return list(self._by_name.values())

    def touch(self, conn_id: int) -> None:
        info = self._by_conn.get(conn_id)
        if info:
            info.last_seen = time.time()
