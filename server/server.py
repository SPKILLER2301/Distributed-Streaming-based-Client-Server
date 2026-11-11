"""Server application lifecycle, authentication and routing. Owner: B."""

from __future__ import annotations
import asyncio
import logging
import time
from common import constants as C, messages as M
from transport.endpoint import Endpoint
from common.bootstrap_integration import load_bootstrap
from .auth import Auth
from .registry import Registry
from .router import Router

log = logging.getLogger(__name__)


class CollabServer:
    def __init__(
        self, host: str, port: int, users_file: str, tcp_port: int | None = None
    ) -> None:
        self.host, self.port = host, port
        self.endpoint = Endpoint(is_server=True)
        self.registry, self.auth = Registry(), Auth(users_file)
        self.router = Router(self.endpoint, self.registry)
        self._peers, self._auth_tasks, self._attempts = {}, {}, {}
        self._maintenance = None
        self.tcp_port = tcp_port
        self._bootstrap = None
        self.endpoint.set_handlers(
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
        )
        self.endpoint.set_message_handler(self._receive_message)

    async def start(self):
        await self.endpoint.start((self.host, self.port))
        try:
            api = load_bootstrap()
            if api:
                udp_port = self.endpoint.local_addr[1]
                self._bootstrap = api[0](
                    self.host,
                    self.tcp_port if self.tcp_port is not None else udp_port,
                    udp_port,
                    C.PROTOCOL_VERSION,
                )
                await self._bootstrap.start()
            else:
                log.warning(
                    "A2 absent: UDP baseline only; TCP rubric Step 2 remains incomplete"
                )
            self._maintenance = asyncio.create_task(self._maintain())
        except BaseException:
            if self._bootstrap:
                await self._bootstrap.stop()
            await self.endpoint.stop()
            raise

    async def stop(self):
        if self._bootstrap:
            await self._bootstrap.stop()
            self._bootstrap = None
        if self._maintenance:
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
            self._maintenance = None
        tasks = list(self._auth_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._auth_tasks.clear()
        await self.endpoint.stop()
        await self.router.stop()

    async def run(self):
        await self.start()
        try:
            await asyncio.Event().wait()
        finally:
            await self.stop()

    async def _maintain(self):
        while True:
            await asyncio.sleep(1)
            for cid, (_, connected) in list(self._peers.items()):
                if (
                    not self.registry.get_by_conn(cid)
                    and time.monotonic() - connected > 10
                ):
                    self.endpoint.close(cid)
            self.router.expire()

    def _on_connect(self, conn_id, addr):
        self._peers[conn_id] = (addr, time.monotonic())

    def _on_disconnect(self, conn_id, reason):
        self.router.cleanup_file_relays(conn_id)  # needs registry entry before removal
        user = self.registry.remove_by_conn(conn_id)
        self._peers.pop(conn_id, None)
        self._attempts.pop(conn_id, None)
        task = self._auth_tasks.pop(conn_id, None)
        if task:
            task.cancel()
        if user:
            self.router.announce(M.USER_LEFT, username=user.username)
            log.info(
                "%s left (%s); online=%d",
                user.username,
                reason,
                len(self.registry.all()),
            )

    def _on_receive(self, conn_id, stream_id, data):
        try:
            if len(data) > 64 * 1024:
                raise ValueError("message too large")
            msg = M.decode(data)
            M.validate_client(msg, stream_id)
            t = msg["type"]
            if t == M.AUTH:
                if conn_id not in self._auth_tasks:
                    self._auth_tasks[conn_id] = asyncio.create_task(
                        self._handle_auth(conn_id, msg)
                    )
                return self._auth_tasks[conn_id]
            user = self.registry.get_by_conn(conn_id)
            if user is None:
                self.router.send_conn(conn_id, 0, M.AUTH_FAIL, reason="chưa đăng nhập")
                return
            self.registry.touch(conn_id)
            if t == M.MSG_BROADCAST:
                self.router.broadcast(user.username, msg["text"])
            elif t == M.MSG_PRIVATE:
                self.router.unicast(user.username, msg["to"], msg["text"])
            elif t in (
                M.FILE_OFFER,
                M.FILE_ACCEPT,
                M.FILE_REJECT,
                M.FILE_DONE,
                M.FILE_ERROR,
            ):
                self.router.forward_signal(user.username, msg)
            elif t == M.LOGOUT:
                self.endpoint.close(conn_id)
            else:
                raise ValueError("unsupported message type")
        except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
            self.router.send_conn(conn_id, 0, M.ERROR, reason=str(exc))

    async def _receive_message(self, message):
        try:
            if message.stream_id >= C.STREAM_FILE_BASE:
                if not self.registry.get_by_conn(message.conn_id):
                    raise ValueError("login required")
                self.registry.touch(message.conn_id)
                await self.router.relay_file_stream(
                    message.conn_id, message.stream_id, message.data
                )
            else:
                auth_task = self._on_receive(
                    message.conn_id, message.stream_id, message.data
                )
                if auth_task is not None:
                    await auth_task
        except (ValueError, KeyError, TypeError, OSError, RuntimeError) as exc:
            self.router.send_conn(message.conn_id, 0, M.ERROR, reason=str(exc))
        finally:
            # Credit returns after processing/rejection or bounded relay enqueue.
            # Each stream has its own consumer, so blocked file relay leaves chat free.
            self.endpoint.consume(message)

    async def _handle_auth(self, conn_id, msg):
        try:
            username, password = msg["username"], msg["password"]
            current = self.registry.get_by_conn(conn_id)
            if current:
                if current.username != username:
                    self.router.send_conn(
                        conn_id, 0, M.AUTH_FAIL, reason="logout before changing account"
                    )
                else:
                    self.router.send_conn(
                        conn_id,
                        0,
                        M.AUTH_OK,
                        username=username,
                        users=self.registry.usernames(),
                    )
                return
            self._attempts[conn_id] = self._attempts.get(conn_id, 0) + 1
            if self._attempts[conn_id] > 5:
                self.router.send_conn(
                    conn_id, 0, M.AUTH_FAIL, reason="too many login attempts; reconnect"
                )
                return
            # PBKDF2 is CPU work: never perform it in a UDP receive callback.
            ok = await asyncio.to_thread(self.auth.verify, username, password)
            if conn_id not in self._peers:
                return
            if not ok:
                self.router.send_conn(
                    conn_id, 0, M.AUTH_FAIL, reason="sai tên hoặc mật khẩu"
                )
            elif not self.registry.add(username, conn_id, self._peers[conn_id][0]):
                self.router.send_conn(
                    conn_id, 0, M.AUTH_FAIL, reason="tài khoản đang được dùng"
                )
            else:
                self.router.send_conn(
                    conn_id,
                    0,
                    M.AUTH_OK,
                    username=username,
                    users=self.registry.usernames(),
                )
                self.router.announce(M.USER_JOINED, username=username)
        finally:
            self._auth_tasks.pop(conn_id, None)
