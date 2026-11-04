"""
Logic client (không có giao diện).                        [Người phụ trách: B]  (Bước 3, 5, 6, 7)

CollabClient dùng Endpoint để nói chuyện với server và đẩy mọi thứ UI cần biết vào `self.events`
(asyncio.Queue các dict). Giao diện web (web_ui/http_server.py) đọc hàng đợi này và phát cho trình duyệt qua SSE.

Các event gửi cho UI (dict có khóa "event"):
    status       {state: "connected"|"disconnected", reason}
    users        {users: [...]}
    chat         {from, text, private}
    file_offer   {file_id, from, name, size}
    file_progress{file_id, direction: "send"|"recv", done, total}
    file_done    {file_id, ok, name, direction}
    error        {reason}

TODO(B): (1) tự kết nối lại khi mất kết nối (vòng lặp thử lại, rồi login lại)  – Bước 7
         (2) từ chối / hủy file; hiển thị tốc độ, RTT từ endpoint.connections[..].rtt
"""
from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Tuple

from common import constants as C
from common import messages as M
from transport.endpoint import Endpoint

from .file_transfer import FileReceiver, FileSender

log = logging.getLogger(__name__)


class CollabClient:
    def __init__(self, server_addr: Tuple[str, int], download_dir: str = "downloads") -> None:
        self.server_addr = server_addr
        self.download_dir = download_dir
        self.endpoint = Endpoint(is_server=False)
        self.endpoint.set_handlers(on_receive=self._on_receive, on_disconnect=self._on_disconnect)
        self.conn_id: Optional[int] = None
        self.username: Optional[str] = None
        self.users: List[str] = []
        self.events: asyncio.Queue = asyncio.Queue()
        self._login_future: Optional[asyncio.Future] = None
        self._outgoing: Dict[str, Tuple[FileSender, str]] = {}      # file_id -> (sender, to)
        self._incoming_offers: Dict[str, dict] = {}                 # file_id -> offer
        self._receivers: Dict[str, FileReceiver] = {}               # file_id -> receiver
        self._recv_streams: Dict[int, str] = {}                     # stream_id của server -> file_id

    # ------------------------------------------------------------ vòng đời
    async def start(self) -> None:
        await self.endpoint.start(("0.0.0.0", 0))
        self.conn_id = await self.endpoint.connect(self.server_addr)
        self._emit("status", state="connected")

    async def login(self, username: str, password: str, timeout: float = 5.0) -> Tuple[bool, str]:
        self._login_future = asyncio.get_running_loop().create_future()
        self._send(C.STREAM_CONTROL, M.AUTH, username=username, password=password)
        try:
            return await asyncio.wait_for(self._login_future, timeout)
        except asyncio.TimeoutError:
            return False, "server không phản hồi"

    async def stop(self) -> None:
        if self.conn_id is not None:
            self._send(C.STREAM_CONTROL, M.LOGOUT)
        await self.endpoint.stop()

    # ------------------------------------------------------------ hành động của người dùng
    def send_chat(self, text: str, to: Optional[str] = None) -> None:
        if to:
            self._send(C.STREAM_CHAT, M.MSG_PRIVATE, to=to, text=text)
        else:
            self._send(C.STREAM_CHAT, M.MSG_BROADCAST, text=text)

    def send_file(self, path: str, to: str) -> str:
        """Gửi lời mời; dữ liệu chỉ được gửi sau khi người nhận FILE_ACCEPT."""
        sender = FileSender(path)
        self._outgoing[sender.file_id] = (sender, to)
        self._send(C.STREAM_CONTROL, M.FILE_OFFER, to=to, file_id=sender.file_id,
                   name=sender.name, size=sender.size, sha256=sender.sha256)
        return sender.file_id

    def accept_file(self, file_id: str) -> None:
        offer = self._incoming_offers.get(file_id)
        if not offer:
            return
        self._receivers[file_id] = FileReceiver(file_id, offer["name"], offer["size"],
                                                offer["sha256"], self.download_dir)
        self._send(C.STREAM_CONTROL, M.FILE_ACCEPT, to=offer["from"], file_id=file_id)

    def reject_file(self, file_id: str) -> None:
        offer = self._incoming_offers.pop(file_id, None)
        if offer:
            self._send(C.STREAM_CONTROL, M.FILE_REJECT, to=offer["from"], file_id=file_id)

    # ------------------------------------------------------------ nhận
    def _on_receive(self, conn_id: int, stream_id: int, data: bytes) -> None:
        if stream_id >= C.STREAM_FILE_BASE:
            self._on_file_stream(stream_id, data)
            return
        try:
            msg = M.decode(data)
        except ValueError:
            return
        t = msg["type"]
        if t == M.AUTH_OK:
            self.username, self.users = msg["username"], msg["users"]
            self._emit("users", users=self.users)
            if self._login_future and not self._login_future.done():
                self._login_future.set_result((True, ""))
        elif t == M.AUTH_FAIL:
            if self._login_future and not self._login_future.done():
                self._login_future.set_result((False, msg.get("reason", "")))
        elif t == M.USER_JOINED:
            if msg["username"] not in self.users:
                self.users.append(msg["username"])
            self._emit("users", users=sorted(self.users))
        elif t == M.USER_LEFT:
            self.users = [u for u in self.users if u != msg["username"]]
            self._emit("users", users=sorted(self.users))
        elif t in (M.MSG_BROADCAST, M.MSG_PRIVATE):
            self._emit("chat", text=msg.get("text", ""), private=(t == M.MSG_PRIVATE), **{"from": msg.get("from")})
        elif t == M.FILE_OFFER:
            self._incoming_offers[msg["file_id"]] = msg
            self._emit("file_offer", file_id=msg["file_id"], name=msg["name"], size=msg["size"],
                       **{"from": msg["from"]})
        elif t == M.FILE_ACCEPT:
            asyncio.get_running_loop().create_task(self._send_file_data(msg["file_id"]))
        elif t == M.FILE_REJECT:
            self._outgoing.pop(msg["file_id"], None)
            self._emit("error", reason=f"{msg.get('from')} từ chối file")
        elif t == M.FILE_DONE:
            sender, _ = self._outgoing.pop(msg["file_id"], (None, None))
            self._emit("file_done", file_id=msg["file_id"], ok=bool(msg.get("ok")),
                       name=sender.name if sender else "", direction="send")
        elif t in (M.FILE_ERROR, M.ERROR):
            self._emit("error", reason=msg.get("reason", ""))

    def _on_file_stream(self, stream_id: int, data: bytes) -> None:
        if stream_id not in self._recv_streams:              # message đầu: FILE_BEGIN
            try:
                msg = M.decode(data)
                file_id = msg["file_id"]
            except (ValueError, KeyError):
                return
            if file_id in self._receivers:
                self._recv_streams[stream_id] = file_id
            return
        rx = self._receivers.get(self._recv_streams[stream_id])
        if rx is None:
            return
        complete = rx.write(data)
        self._emit("file_progress", file_id=rx.file_id, direction="recv", done=rx.received, total=rx.size)
        if complete:
            ok = rx.finish()
            offer = self._incoming_offers.pop(rx.file_id, {})
            self._receivers.pop(rx.file_id, None)
            self._recv_streams.pop(stream_id, None)
            self._send(C.STREAM_CONTROL, M.FILE_DONE, to=offer.get("from"), file_id=rx.file_id, ok=ok)
            self._emit("file_done", file_id=rx.file_id, ok=ok, name=offer.get("name", ""), direction="recv")

    async def _send_file_data(self, file_id: str) -> None:
        item = self._outgoing.get(file_id)
        if item is None or self.conn_id is None:
            return
        sender, to = item
        sid = self.endpoint.open_stream(self.conn_id)
        self._send(sid, M.FILE_BEGIN, to=to, file_id=file_id, size=sender.size)
        for n, chunk in enumerate(sender.chunks(), 1):
            self.endpoint.send(self.conn_id, sid, chunk)
            self._emit("file_progress", file_id=file_id, direction="send", done=sender.sent, total=sender.size)
            if n % 64 == 0:                                   # TODO(A): thay bằng backpressure thật
                await asyncio.sleep(0.01)

    def _on_disconnect(self, conn_id: int, reason: str) -> None:
        self.conn_id = None
        self._emit("status", state="disconnected", reason=reason)
        # TODO(B): tự kết nối lại + đăng nhập lại ở đây

    # ------------------------------------------------------------ tiện ích
    def _send(self, stream_id: int, msg_type: str, **fields) -> None:
        if self.conn_id is not None:
            self.endpoint.send(self.conn_id, stream_id, M.encode(msg_type, **fields))

    def _emit(self, event: str, **fields) -> None:
        self.events.put_nowait({"event": event, **fields})
