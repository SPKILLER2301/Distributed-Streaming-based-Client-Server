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
import inspect
from collections import deque
from typing import Callable, Dict, Optional, Tuple

from common import constants as C
from common.packet import Packet, PacketError, decode, encode

from .connection import Connection
from .congestion import make_controller
from .crypto import CryptoError, KeyExchange
from .flow import FlowLimits, QueueFull, ConnectionClosed

log = logging.getLogger(__name__)
Addr = Tuple[str, int]


class Endpoint(asyncio.DatagramProtocol):
    def __init__(self, is_server: bool = False, *, flow_limits: FlowLimits | None = None,
                 congestion_algorithm: str = "cubic") -> None:
        make_controller(congestion_algorithm)  # Reject invalid config before opening a socket.
        self.congestion_algorithm = congestion_algorithm
        self.is_server = is_server
        self.connections: Dict[int, Connection] = {}
        self.link = None                       # hook cho test: đối tượng có .send(real_sendto, data, addr)
        self.flow_limits = flow_limits or FlowLimits()
        self._message_handler = None
        self._consumer_queues = {}
        self._consumers = {}
        self._consumer_tasks = set()
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._tick_task: Optional[asyncio.Task] = None
        self._on_connect = self._on_receive = self._on_disconnect = None
        # server: addr -> (conn_id, client_pub, reply_bytes)  (để trả lời lại HELLO bị gửi lặp)
        self._hello_by_addr: Dict[Addr, Tuple[int, bytes, bytes]] = {}
        # client: handshake đang chờ
        self._pending: Optional[Tuple[Addr, KeyExchange, asyncio.Future]] = None

    # ------------------------------------------------------------------ API công khai
    def set_handlers(self, on_connect=None, on_receive=None, on_disconnect=None) -> None:
        if on_receive is not None and self._message_handler is not None:
            raise ValueError("choose either sync on_receive or async message handler")
        self._on_connect, self._on_receive, self._on_disconnect = on_connect, on_receive, on_disconnect

    def set_message_handler(self, handler) -> None:
        if self._transport is not None:
            raise RuntimeError("set receive mode before start")
        if handler is not None and (self._on_receive is not None or not inspect.iscoroutinefunction(handler)):
            raise ValueError("provide an async handler, exclusive of on_receive")
        self._message_handler = handler

    async def start(self, local_addr: Addr = ("0.0.0.0", 0)) -> None:
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(lambda: self, local_addr=local_addr)
        self._tick_task = asyncio.create_task(self._tick_loop())

    @property
    def local_addr(self) -> Addr:
        return self._transport.get_extra_info("sockname")

    async def connect(self, remote_addr: Addr) -> int:
        """Client: handshake với server, trả về conn_id. Raise ConnectionError nếu không có phản hồi."""
        if self._pending is not None:
            raise ConnectionError("another handshake is pending")
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
            if not fut.done():
                fut.cancel()

    def send(self, conn_id: int, stream_id: int, data: bytes) -> bool:
        conn = self.connections.get(conn_id)
        if conn is None or conn.closed:
            return False
        try:
            conn.send_message(stream_id, data)
        except QueueFull:
            return False
        return True

    def _connection(self, conn_id):
        conn = self.connections.get(conn_id)
        if conn is None or conn.closed:
            raise ConnectionClosed("connection unavailable")
        return conn

    async def send_message(self, conn_id, stream_id, data):
        return await self._connection(conn_id).enqueue_message(stream_id, data)

    def consume(self, message):
        conn = self.connections.get(message.conn_id)
        if conn is not None:
            conn.consume(message)

    async def cancel_stream(self, conn_id, stream_id, reason="cancelled"):
        await self._connection(conn_id).cancel_stream(stream_id, reason)

    def get_stats(self, conn_id):
        return self._connection(conn_id).get_stats()

    def get_peer_addr(self, conn_id):
        return self._connection(conn_id).remote_addr

    def open_stream(self, conn_id: int) -> int:
        return self._connection(conn_id).open_stream()

    def close(self, conn_id: int, reason="closed locally") -> None:
        conn = self.connections.get(conn_id)
        if conn:
            conn.close(reason)

    async def stop(self) -> None:
        for conn in list(self.connections.values()):
            conn.close("endpoint_stopped")
        if self._pending is not None:
            fut = self._pending[2]
            if not fut.done():
                fut.set_exception(ConnectionClosed("endpoint_stopped"))
        if self._tick_task:
            self._tick_task.cancel()
            await asyncio.gather(self._tick_task, return_exceptions=True)
            self._tick_task = None
        current = asyncio.current_task()
        tasks = [task for task in self._consumer_tasks if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._consumer_tasks.clear()
        self._consumers.clear()
        self._consumer_queues.clear()
        if self._transport:
            self._transport.close()
            self._transport = None

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
                          on_message=self._receive_sync,
                          on_closed=self._conn_closed, flow_limits=self.flow_limits,
                          on_delivery=self._dispatch if self._message_handler is not None else None,
                          on_stream_reset=self._stream_reset,
                          congestion_algorithm=self.congestion_algorithm)
        self.connections[conn_id] = conn
        return conn

    def _receive_sync(self, cid, sid, data):
        if self._on_receive is not None:
            self._on_receive(cid, sid, data)

    def _dispatch(self, message):
        key = message.conn_id, message.stream_id
        self._consumer_queues.setdefault(key, deque()).append(message)
        if key not in self._consumers:
            task = asyncio.create_task(self._consume_stream(key))
            self._consumers[key] = task
            self._consumer_tasks.add(task)
            task.add_done_callback(self._consumer_tasks.discard)

    async def _consume_stream(self, key):
        try:
            queue = self._consumer_queues[key]
            while queue:
                message = queue.popleft()
                await self._message_handler(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("async receive handler failed")
            conn = self.connections.get(key[0])
            if conn is not None:
                conn.request_stop(key[1])
        finally:
            self._consumers.pop(key, None)
            self._consumer_queues.pop(key, None)

    def _stream_reset(self, conn_id, sid):
        key = conn_id, sid
        task = self._consumers.get(key)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        queue = self._consumer_queues.pop(key, None)
        if queue is not None:
            queue.clear()

    def _conn_closed(self, conn_id: int, reason: str) -> None:
        conn = self.connections.pop(conn_id, None)
        if conn is None:
            return
        for key in list(self._consumer_queues):
            if key[0] == conn_id:
                self._stream_reset(*key)
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
