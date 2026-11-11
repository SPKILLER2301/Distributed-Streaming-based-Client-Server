"""
Định tuyến: broadcast, unicast, chuyển tiếp tín hiệu và dữ liệu file.   [Người phụ trách: B]  (Bước 5, 6)

Router chỉ biết Endpoint (để gửi) và Registry (để tra user) – không đụng tới socket.

ĐÃ CÀI SẴN: broadcast, unicast, chuyển tiếp tín hiệu FILE_* (OFFER/ACCEPT/REJECT/DONE),
            chuyển tiếp stream file (FILE_BEGIN + các chunk) sang stream mới tới người nhận.
TODO(B): khi một bên rớt giữa chừng, gửi FILE_ERROR cho bên còn lại (xem cleanup_file_relays).
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple

from common import constants as C
from common import messages as M
from transport.endpoint import Endpoint

from .registry import Registry

log = logging.getLogger(__name__)


class Router:
    def __init__(self, endpoint: Endpoint, registry: Registry) -> None:
        self.endpoint = endpoint
        self.registry = registry
        # (conn_id gửi, stream_id gửi) -> [conn_id nhận, stream_id nhận, số byte còn lại]
        self._relays: Dict[Tuple[int, int], List[int]] = {}

    # --------------------------------------------------------------- gửi tiện lợi
    def send_conn(self, conn_id: int, stream_id: int, msg_type: str, **fields) -> None:
        self.endpoint.send(conn_id, stream_id, M.encode(msg_type, **fields))

    def send_user(self, username: str, stream_id: int, msg_type: str, **fields) -> bool:
        info = self.registry.get_by_name(username)
        if info is None:
            return False
        self.send_conn(info.conn_id, stream_id, msg_type, **fields)
        return True

    # --------------------------------------------------------------- chat
    def broadcast(self, sender: str, text: str) -> None:
        for user in self.registry.all():
            if user.username != sender:
                self.send_conn(user.conn_id, C.STREAM_CHAT, M.MSG_BROADCAST, **{"from": sender, "text": text})

    def unicast(self, sender: str, to: str, text: str) -> bool:
        ok = self.send_user(to, C.STREAM_CHAT, M.MSG_PRIVATE, **{"from": sender, "text": text})
        if not ok:
            self.send_user(sender, C.STREAM_CONTROL, M.ERROR, reason=f"người dùng '{to}' không online")
        return ok

    def announce(self, msg_type: str, **fields) -> None:
        """Gửi cho tất cả user online (USER_JOINED / USER_LEFT ...)."""
        for user in self.registry.all():
            self.send_conn(user.conn_id, C.STREAM_CONTROL, msg_type, **fields)

    # --------------------------------------------------------------- file
    def forward_signal(self, sender: str, msg: dict) -> None:
        """FILE_OFFER / ACCEPT / REJECT / DONE: đổi 'to' thành 'from' rồi chuyển cho người nhận."""
        to = msg.pop("to", None)
        msg_type = msg.pop("type")
        msg["from"] = sender
        if not to or not self.send_user(to, C.STREAM_CONTROL, msg_type, **msg):
            self.send_user(sender, C.STREAM_CONTROL, M.FILE_ERROR,
                           file_id=msg.get("file_id"), reason=f"không gửi được tới '{to}'")

    async def relay_file_stream(self, conn_id: int, stream_id: int, data: bytes) -> None:
        """Tin nhắn trên stream file (>=2). Message đầu = FILE_BEGIN (JSON), các message sau = chunk thô."""
        key = (conn_id, stream_id)
        relay = self._relays.get(key)
        if relay is None:                                   # message đầu tiên: FILE_BEGIN
            try:
                msg = M.decode(data)
                assert msg["type"] == M.FILE_BEGIN
                sender = self.registry.get_by_conn(conn_id)
                dest = self.registry.get_by_name(msg["to"])
                size = int(msg["size"])
            except (ValueError, KeyError, AssertionError, TypeError):
                log.warning("stream file %s/%s bắt đầu sai", conn_id, stream_id)
                return
            if sender is None or dest is None:
                return
            dst_stream = self.endpoint.open_stream(dest.conn_id)
            self._relays[key] = [dest.conn_id, dst_stream, size]
            await self.endpoint.send_message(dest.conn_id, dst_stream, M.encode(M.FILE_BEGIN,
                **{"from": sender.username, "file_id": msg["file_id"], "size": size}))
            return
        dst_conn, dst_stream, remaining = relay
        await self.endpoint.send_message(dst_conn, dst_stream, data)
        relay[2] = remaining - len(data)
        if relay[2] <= 0:
            del self._relays[key]                           # file đã chuyển xong

    def cleanup_file_relays(self, conn_id: int) -> None:
        """Gọi khi một kết nối rớt: xóa các relay liên quan.
        TODO(B): với mỗi relay bị hủy, gửi FILE_ERROR cho người còn lại (cần lưu thêm file_id và tên user)."""
        for key in [k for k, v in self._relays.items() if k[0] == conn_id or v[0] == conn_id]:
            del self._relays[key]
