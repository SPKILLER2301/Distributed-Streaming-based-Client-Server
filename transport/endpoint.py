"""
Endpoint: socket UDP + handshake + phân phối gói tới đúng Connection.   [Người phụ trách: A]  (Bước 2, 4, 7)

ĐÂY LÀ API DUY NHẤT mà tầng ứng dụng (B) được gọi – đừng để server/client chạm vào socket hay Connection:

    ep = Endpoint(is_server=True)                       # hoặc False cho client
    ep.set_handlers(on_connect=..., on_receive=..., on_disconnect=...)   # callback ĐỒNG BỘ (sync)
    await ep.start(("0.0.0.0", 9000))                   # client: ("0.0.0.0", 0)
    conn_id = await ep.connect(("127.0.0.1", 9000))     # chỉ client
    ep.send(conn_id, stream_id, data)  -> bool          # data: bytes, 1 lần gọi = 1 message
    sid = ep.open_stream(conn_id)                       # stream mới cho 1 file
    ep.close(conn_id)

    on_connect(conn_id, addr)                  # chỉ phía server, sau khi handshake xong
    on_receive(conn_id, stream_id, data)       # nhận đủ 1 message, ĐÚNG THỨ TỰ trong stream
    on_disconnect(conn_id, reason)             # đối phương đóng / timeout / lỗi

Concurrency (Bước 4): dùng asyncio – một event loop xử lý mọi client, không chặn.
Resilience (Bước 7): mọi xử lý gói bọc try/except, gói hỏng chỉ bị bỏ, server không bao giờ crash.

ĐÃ CÀI SẴN: handshake X25519 (có gửi lại HELLO), bảng kết nối, vòng lặp timer, hook mô phỏng mất gói (`self.link`).
TODO(A): giới hạn số kết nối tối đa; chống HELLO flood (tùy chọn); xử lý lỗi UDP riêng của Windows
         (WinError 10054 khi gửi tới cổng đã đóng – hiện chỉ ghi log trong error_received).
"""
from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from typing import Callable, Dict, Optional, Tuple

from common import constants as C
from common.packet import Packet, PacketError, decode, encode

from .connection import Connection
from .crypto import CryptoError, KeyExchange

log = logging.getLogger(__name__)
Addr = Tuple[str, int]


class Endpoint(asyncio.DatagramProtocol):
    def __init__(self, is_server: bool = False) -> None:
        self.is_server = is_server
        self.connections: Dict[int, Connection] = {}
        self.link = None                       # hook cho test: đối tượng có .send(real_sendto, data, addr)
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._tick_task: Optional[asyncio.Task] = None
        self._on_connect = self._on_receive = self._on_disconnect = None
        # server: addr -> (conn_id, client_pub, reply_bytes)  (để trả lời lại HELLO bị gửi lặp)
        self._hello_by_addr: Dict[Addr, Tuple[int, bytes, bytes]] = {}
        # client: handshake đang chờ
        self._pending: Optional[Tuple[Addr, KeyExchange, asyncio.Future]] = None

    # ------------------------------------------------------------------ API công khai
    def set_handlers(self, on_connect=None, on_receive=None, on_disconnect=None) -> None:
        self._on_connect, self._on_receive, self._on_disconnect = on_connect, on_receive, on_disconnect

    async def start(self, local_addr: Addr = ("0.0.0.0", 0)) -> None:
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(lambda: self, local_addr=local_addr)
        self._tick_task = asyncio.create_task(self._tick_loop())

    @property
    def local_addr(self) -> Addr:
        return self._transport.get_extra_info("sockname")

    async def connect(self, remote_addr: Addr) -> int:
        """Client: handshake với server, trả về conn_id. Raise ConnectionError nếu không có phản hồi."""
        remote = (socket.gethostbyname(remote_addr[0]), remote_addr[1])
        kx = KeyExchange()
        fut = asyncio.get_running_loop().create_future()
        self._pending = (remote, kx, fut)
        hello = encode(Packet(C.PKT_HELLO, payload=kx.public_bytes))
        try:
            for _ in range(C.HANDSHAKE_RETRIES):
                self.sendto(hello, remote)
                try:
                    return await asyncio.wait_for(asyncio.shield(fut), C.HANDSHAKE_TIMEOUT)
                except asyncio.TimeoutError:
                    continue
            raise ConnectionError(f"không kết nối được tới {remote_addr}")
        finally:
            self._pending = None

    def send(self, conn_id: int, stream_id: int, data: bytes) -> bool:
        conn = self.connections.get(conn_id)
        if conn is None or conn.closed:
            return False
        conn.send_message(stream_id, data)
        return True

    def open_stream(self, conn_id: int) -> int:
        return self.connections[conn_id].open_stream()

    def close(self, conn_id: int) -> None:
        conn = self.connections.get(conn_id)
        if conn:
            conn.close()

    async def stop(self) -> None:
        for conn in list(self.connections.values()):
            conn.close()
        if self._tick_task:
            self._tick_task.cancel()
        if self._transport:
            self._transport.close()

    # ------------------------------------------------------------------ gửi thô
    def sendto(self, data: bytes, addr: Addr) -> None:
        if self._transport is None:
            return
        try:
            if self.link is not None:
                self.link.send(self._transport.sendto, data, addr)
            else:
                self._transport.sendto(data, addr)
        except OSError as exc:
            log.debug("sendto lỗi: %s", exc)

    # ------------------------------------------------------------------ asyncio.DatagramProtocol
    def connection_made(self, transport) -> None:
        self._transport = transport

    def error_received(self, exc: Exception) -> None:
        log.debug("lỗi UDP: %s", exc)          # ví dụ ICMP port unreachable – bỏ qua, timer sẽ lo

    def datagram_received(self, data: bytes, addr: Addr) -> None:
        try:
            pkt = decode(data)
        except PacketError:
            return                              # gói rác: bỏ
        try:
            if pkt.ptype == C.PKT_HELLO and self.is_server:
                self._handle_hello(pkt, addr)
            elif pkt.ptype == C.PKT_HELLO_REPLY and not self.is_server:
                self._handle_hello_reply(pkt, addr)
            else:
                conn = self.connections.get(pkt.conn_id)
                if conn is not None and conn.remote_addr == addr:
                    conn.handle_packet(pkt)
        except Exception:                       # Bước 7: không để 1 gói xấu làm sập server
            log.exception("lỗi khi xử lý gói từ %s", addr)

    # ------------------------------------------------------------------ handshake
    def _handle_hello(self, pkt: Packet, addr: Addr) -> None:
        known = self._hello_by_addr.get(addr)
        if known and known[1] == pkt.payload and known[0] in self.connections:
            self.sendto(known[2], addr)         # HELLO gửi lặp (mất REPLY): trả lời lại y hệt
            return
        if known and known[0] in self.connections:
            self.connections[known[0]].close()  # client khởi động lại với khóa mới: bỏ kết nối cũ
        kx = KeyExchange()
        try:
            cipher = kx.derive_cipher(pkt.payload, is_client=False)
        except CryptoError:
            return
        conn_id = int.from_bytes(os.urandom(4), "big") or 1
        while conn_id in self.connections:
            conn_id = int.from_bytes(os.urandom(4), "big") or 1
        reply = encode(Packet(C.PKT_HELLO_REPLY, conn_id=conn_id, payload=kx.public_bytes))
        self._hello_by_addr[addr] = (conn_id, pkt.payload, reply)
        self._make_connection(conn_id, addr, cipher)
        self.sendto(reply, addr)
        self._fire(self._on_connect, conn_id, addr)

    def _handle_hello_reply(self, pkt: Packet, addr: Addr) -> None:
        if self._pending is None:
            return
        remote, kx, fut = self._pending
        if addr != remote or fut.done():
            return
        cipher = kx.derive_cipher(pkt.payload, is_client=True)
        self._make_connection(pkt.conn_id, addr, cipher)
        fut.set_result(pkt.conn_id)

    def _make_connection(self, conn_id: int, addr: Addr, cipher) -> Connection:
        conn = Connection(conn_id, addr, cipher, self.sendto,
                          on_message=lambda c, s, d: self._fire(self._on_receive, c, s, d),
                          on_closed=self._conn_closed)
        self.connections[conn_id] = conn
        return conn

    def _conn_closed(self, conn_id: int, reason: str) -> None:
        conn = self.connections.pop(conn_id, None)
        if conn is None:
            return
        info = self._hello_by_addr.get(conn.remote_addr)
        if info and info[0] == conn_id:
            del self._hello_by_addr[conn.remote_addr]
        self._fire(self._on_disconnect, conn_id, reason)

    # ------------------------------------------------------------------ tiện ích
    @staticmethod
    def _fire(callback, *args) -> None:
        if callback is None:
            return
        try:
            callback(*args)
        except Exception:
            log.exception("callback ứng dụng lỗi")

    async def _tick_loop(self) -> None:
        while True:
            await asyncio.sleep(C.TICK_INTERVAL)
            now = time.monotonic()
            for conn in list(self.connections.values()):
                try:
                    conn.on_tick(now)
                except Exception:
                    log.exception("on_tick lỗi")
