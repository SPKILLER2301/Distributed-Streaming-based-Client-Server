"""
Server: nối Endpoint (transport) với Auth / Registry / Router.   [Người phụ trách: B]  (Bước 3, 4, 5, 7)

Luồng xử lý một message nhận được (xem _on_receive):
    stream >= 2          -> dữ liệu file  -> router.relay_file_stream
    AUTH                 -> _handle_auth
    (chưa đăng nhập)     -> từ chối
    MSG_BROADCAST/PRIVATE-> router
    FILE_*               -> router.forward_signal
    LOGOUT               -> đóng kết nối

Concurrency (Bước 4): toàn bộ chạy trên 1 event loop asyncio; callback không được chặn lâu.
Resilience (Bước 7): message sai định dạng -> trả ERROR, KHÔNG raise; client rớt -> _on_disconnect dọn dẹp.

TODO(B): giới hạn kích thước tin nhắn/tốc độ gửi (chống spam); log thống kê số client online.
"""
from __future__ import annotations

import asyncio
import logging

from common import constants as C
from common import messages as M
from transport.endpoint import Endpoint

from .auth import Auth
from .registry import Registry
from .router import Router

log = logging.getLogger(__name__)


class CollabServer:
    def __init__(self, host: str, port: int, users_file: str) -> None:
        self.host, self.port = host, port
        self.endpoint = Endpoint(is_server=True)
        self.registry = Registry()
        self.auth = Auth(users_file)
        self.router = Router(self.endpoint, self.registry)
        self.endpoint.set_handlers(on_connect=self._on_connect,
                                   on_disconnect=self._on_disconnect)
        self.endpoint.set_message_handler(self._receive_message)

    async def run(self) -> None:
        await self.endpoint.start((self.host, self.port))
        log.info("Server lắng nghe UDP %s:%s", self.host, self.port)
        await asyncio.Event().wait()                      # chạy mãi

    # ---------------------------------------------------------------- callbacks từ transport
    def _on_connect(self, conn_id: int, addr) -> None:
        log.info("kết nối mới conn=%s từ %s (chưa đăng nhập)", conn_id, addr)

    def _on_disconnect(self, conn_id: int, reason: str) -> None:
        user = self.registry.remove_by_conn(conn_id)
        self.router.cleanup_file_relays(conn_id)
        if user:
            log.info("%s rời đi (%s)", user.username, reason)
            self.router.announce(M.USER_LEFT, username=user.username)

    async def _receive_message(self, message) -> None:
        # Transfer ownership only after the bounded outgoing queue accepts the
        # relay chunk. Per-stream dispatch keeps a blocked file off chat's path.
        if message.stream_id >= C.STREAM_FILE_BASE:
            if self.registry.get_by_conn(message.conn_id):
                await self.router.relay_file_stream(message.conn_id, message.stream_id, message.data)
        else:
            self._on_receive(message.conn_id, message.stream_id, message.data)
        self.endpoint.consume(message)

    def _on_receive(self, conn_id: int, stream_id: int, data: bytes) -> None:
        try:
            msg = M.decode(data)
        except ValueError:
            self.router.send_conn(conn_id, C.STREAM_CONTROL, M.ERROR, reason="message sai định dạng")
            return

        mtype = msg["type"]
        if mtype == M.AUTH:
            self._handle_auth(conn_id, msg)
            return
        user = self.registry.get_by_conn(conn_id)
        if user is None:
            self.router.send_conn(conn_id, C.STREAM_CONTROL, M.AUTH_FAIL, reason="chưa đăng nhập")
            return
        self.registry.touch(conn_id)

        if mtype == M.MSG_BROADCAST:
            self.router.broadcast(user.username, str(msg.get("text", "")))
        elif mtype == M.MSG_PRIVATE:
            self.router.unicast(user.username, str(msg.get("to", "")), str(msg.get("text", "")))
        elif mtype in (M.FILE_OFFER, M.FILE_ACCEPT, M.FILE_REJECT, M.FILE_DONE):
            self.router.forward_signal(user.username, msg)
        elif mtype == M.LOGOUT:
            self.endpoint.close(conn_id)
        else:
            self.router.send_conn(conn_id, C.STREAM_CONTROL, M.ERROR, reason=f"type không hỗ trợ: {mtype}")

    def _handle_auth(self, conn_id: int, msg: dict) -> None:
        username, password = str(msg.get("username", "")), str(msg.get("password", ""))
        if not self.auth.verify(username, password):
            self.router.send_conn(conn_id, C.STREAM_CONTROL, M.AUTH_FAIL, reason="sai tên hoặc mật khẩu")
            return
        # Lấy addr từ connection để lưu vào registry (yêu cầu Bước 3: IP + port)
        addr = self.endpoint.get_peer_addr(conn_id)
        if not self.registry.add(username, conn_id, addr):
            self.router.send_conn(conn_id, C.STREAM_CONTROL, M.AUTH_FAIL, reason="tài khoản đang được dùng")
            return
        self.router.send_conn(conn_id, C.STREAM_CONTROL, M.AUTH_OK,
                              username=username, users=self.registry.usernames())
        self.router.announce(M.USER_JOINED, username=username)   # tất cả (kể cả người mới) cập nhật danh sách
        log.info("%s đăng nhập từ %s", username, addr)
