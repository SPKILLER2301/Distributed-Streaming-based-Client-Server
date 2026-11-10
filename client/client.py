"""Client application, reconnect supervisor and file lifecycle. Owner: B.

On current transport v1, send progress is QUEUED bytes, not ACKed bytes.
Only FILE_DONE from the receiver confirms end-to-end success.
"""

from __future__ import annotations
import asyncio
import logging
import random
from pathlib import Path
from common import constants as C, messages as M
from transport.endpoint import Endpoint
from .file_transfer import FileReceiver, FileSender
from common.bootstrap_integration import load_bootstrap

log = logging.getLogger(__name__)


class CollabClient:
    def __init__(self, server_addr, download_dir="downloads"):
        self.server_addr, self.download_dir = server_addr, download_dir
        self.endpoint = Endpoint(is_server=False)
        self.endpoint.set_handlers(
            on_receive=self._on_receive, on_disconnect=self._on_disconnect
        )
        self.conn_id = self.username = None
        self.users = []
        self.events = asyncio.Queue(maxsize=2048)
        self._login_future = None
        self._login_lock = asyncio.Lock()
        self._credentials = None
        self._stopping = False
        self._auto_reconnect = True
        self._reconnect_task = None
        self._outgoing, self._incoming_offers, self._receivers, self._recv_streams = (
            {},
            {},
            {},
            {},
        )
        self._send_tasks, self._owned_paths = {}, {}
        self._sending = set()

    async def start(self):
        self._stopping = False
        await self.endpoint.start(("0.0.0.0", 0))
        try:
            await self._connect()
        except (ConnectionError, OSError) as exc:
            self._emit("status", state="disconnected", reason=str(exc))
            self._schedule_reconnect()

    async def _connect(self):
        api = load_bootstrap()
        if api:
            udp_addr = await api[1](
                self.server_addr, version=C.PROTOCOL_VERSION, timeout=3.0
            )
        else:
            udp_addr = self.server_addr
            log.warning("A2 absent: UDP baseline only; TCP handshake not implemented")
        self.conn_id = await self.endpoint.connect(udp_addr)
        self._emit("status", state="connected")

    async def login(self, username, password, timeout=5.0):
        if not isinstance(username, str) or not isinstance(password, str):
            return False, "invalid credentials"
        async with self._login_lock:
            if self.conn_id is None:
                self._auto_reconnect = True
                self._schedule_reconnect()
                return False, "server offline; đang kết nối lại"
            self._login_future = asyncio.get_running_loop().create_future()
            try:
                self._send(0, M.AUTH, username=username, password=password)
                result = await asyncio.wait_for(self._login_future, timeout)
                if result[0]:
                    self._credentials = (
                        username,
                        password,
                    )  # memory only; erased by logout/stop
                return result
            except (asyncio.TimeoutError, ConnectionError):
                return False, "server không phản hồi hoặc kết nối đã ngắt"
            finally:
                self._login_future = None

    async def logout(self):
        self._auto_reconnect = False
        if self._reconnect_task:
            self._reconnect_task.cancel()
            await asyncio.gather(self._reconnect_task, return_exceptions=True)
            self._reconnect_task = None
        self._credentials = None
        self.username = None
        self.users = []
        self._cleanup_files("logout")
        if self.conn_id is not None:
            try:
                self._send(0, M.LOGOUT)
            finally:
                self.endpoint.close(self.conn_id)

    async def stop(self):
        self._stopping = True
        self._credentials = None
        tasks = list(self._send_tasks.values())
        if self._reconnect_task:
            tasks.append(self._reconnect_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._reconnect_task = None
        self._cleanup_files("client stopped")
        await self.endpoint.stop()
        self.conn_id = self.username = None
        self.users = []

    def _schedule_reconnect(self):
        if (
            self._auto_reconnect
            and not self._stopping
            and (self._reconnect_task is None or self._reconnect_task.done())
        ):
            self._reconnect_task = asyncio.create_task(self._reconnect())

    async def _reconnect(self):
        attempt = 0
        while not self._stopping and self.conn_id is None:
            delay = min(16, 0.5 * 2 ** min(attempt, 5)) * random.uniform(0.8, 1.2)
            self._emit("status", state="reconnecting", reason=f"retry in {delay:.1f}s")
            await asyncio.sleep(delay)
            try:
                await self._connect()
                if self._credentials:
                    ok, reason = await self.login(*self._credentials)
                    if not ok:
                        self._credentials = None
                        self._emit("error", reason=f"login again: {reason}")
                if self.conn_id is not None:
                    return
            except (ConnectionError, OSError) as exc:
                self._emit("error", reason=str(exc))
            attempt += 1

    def send_chat(self, text, to=None):
        if not self.username:
            raise ConnectionError("login required")
        if not isinstance(text, str) or not 1 <= len(text) <= 4096:
            raise ValueError("chat must contain 1..4096 characters")
        self._send(
            1,
            M.MSG_PRIVATE if to else M.MSG_BROADCAST,
            **({"to": to} if to else {}),
            text=text,
        )

    def send_file(self, path, to, *, delete_after=False):
        return self.offer_sender(FileSender(path), to, delete_after=delete_after)

    def offer_sender(self, sender, to, *, delete_after=False):
        if not self.username or not to or to == self.username:
            raise ValueError("login and select another user")
        if len(self._outgoing) >= M.MAX_TRANSFERS or sender.size > M.MAX_FILE_SIZE:
            raise ValueError("file/transfer limit exceeded")
        message = dict(
            type=M.FILE_OFFER,
            to=to,
            file_id=sender.file_id,
            name=sender.name,
            size=sender.size,
            sha256=sender.sha256,
        )
        M.validate_client(message, 0)
        self._send(0, M.FILE_OFFER, **{k: v for k, v in message.items() if k != "type"})
        self._outgoing[sender.file_id] = (sender, to)
        if delete_after:
            self._owned_paths[sender.file_id] = sender.path
        return sender.file_id

    def accept_file(self, file_id):
        offer = self._incoming_offers.get(file_id)
        if not offer or file_id in self._receivers:
            raise ValueError("unknown or already accepted offer")
        rx = FileReceiver(
            file_id, offer["name"], offer["size"], offer["sha256"], self.download_dir
        )
        try:
            self._send(0, M.FILE_ACCEPT, to=offer["from"], file_id=file_id)
        except BaseException:
            rx.abort()
            raise
        self._receivers[file_id] = rx

    def reject_file(self, file_id):
        offer = self._incoming_offers.get(file_id)
        if not offer or file_id in self._receivers:
            raise ValueError("unknown or already accepted offer")
        self._send(0, M.FILE_REJECT, to=offer["from"], file_id=file_id)
        self._incoming_offers.pop(file_id, None)

    def _on_receive(self, conn_id, stream_id, data):
        if conn_id != self.conn_id:
            return
        try:
            if stream_id >= C.STREAM_FILE_BASE:
                self._on_file_stream(stream_id, data)
                return
            msg = M.decode(data)
            t = msg["type"]
            if t == M.AUTH_OK:
                self.username, self.users = msg["username"], msg["users"]
                self._emit("users", users=self.users)
                if self._login_future and not self._login_future.done():
                    self._login_future.set_result((True, ""))
            elif t == M.AUTH_FAIL:
                if self._login_future and not self._login_future.done():
                    self._login_future.set_result((False, msg.get("reason", "")))
            elif t in (M.USER_JOINED, M.USER_LEFT):
                self.users = sorted(
                    (set(self.users) | {msg["username"]})
                    if t == M.USER_JOINED
                    else (set(self.users) - {msg["username"]})
                )
                self._emit("users", users=self.users)
            elif t in (M.MSG_BROADCAST, M.MSG_PRIVATE):
                self._emit(
                    "chat",
                    text=msg["text"],
                    private=t == M.MSG_PRIVATE,
                    **{"from": msg["from"]},
                )
            elif t == M.FILE_OFFER:
                if len(self._incoming_offers) >= M.MAX_TRANSFERS:
                    self._send(0, M.FILE_REJECT, to=msg["from"], file_id=msg["file_id"])
                    return
                self._incoming_offers[msg["file_id"]] = msg
                self._emit(
                    "file_offer",
                    **{k: msg[k] for k in ("file_id", "name", "size", "from")},
                )
            elif t == M.FILE_ACCEPT:
                fid = msg["file_id"]
                if fid in self._outgoing and fid not in self._sending:
                    self._sending.add(fid)
                    self._send_tasks[fid] = asyncio.create_task(
                        self._send_file_data(fid)
                    )
            elif t == M.FILE_REJECT:
                self._drop_file(msg["file_id"])
                self._emit("error", reason=f"{msg.get('from')} từ chối file")
            elif t == M.FILE_DONE:
                fid = msg["file_id"]
                sender, _ = self._outgoing.get(fid, (None, None))
                if sender:
                    self._emit(
                        "file_done",
                        file_id=fid,
                        ok=msg["ok"],
                        name=sender.name,
                        direction="send",
                    )
                    self._drop_file(fid)
            elif t in (M.FILE_ERROR, M.ERROR):
                if t == M.FILE_ERROR:
                    self._drop_file(msg["file_id"])
                self._emit("error", reason=msg.get("reason", ""))
        except (ValueError, KeyError, TypeError, OSError, ConnectionError) as exc:
            self._emit("error", reason=str(exc))

    def _on_file_stream(self, stream_id, data):
        fid = self._recv_streams.get(stream_id)
        if fid is None:
            msg = M.decode(data)
            fid = msg["file_id"]
            rx, offer = self._receivers.get(fid), self._incoming_offers.get(fid)
            if (
                msg["type"] != M.FILE_BEGIN
                or not rx
                or not offer
                or fid in self._recv_streams.values()
                or msg.get("from") != offer["from"]
                or msg.get("size") != rx.size
            ):
                raise ValueError("BEGIN without matching accepted offer")
            self._recv_streams[stream_id] = fid
            if rx.size == 0:
                self._finish_file(stream_id, rx)
            return
        rx = self._receivers[fid]
        try:
            complete = rx.write(data)
            self._emit(
                "file_progress",
                file_id=fid,
                direction="recv",
                done=rx.received,
                total=rx.size,
                stage="received",
            )
            if complete:
                self._finish_file(stream_id, rx)
        except (ValueError, OSError) as exc:
            offer = self._incoming_offers.get(fid)
            if offer:
                self._notify_file_error(offer["from"], fid, "receive failed")
            self._drop_file(fid)
            raise ValueError("file receive failed") from exc

    def _finish_file(self, stream_id, rx):
        ok = rx.finish()
        offer = self._incoming_offers.pop(rx.file_id)
        self._receivers.pop(rx.file_id, None)
        self._recv_streams.pop(stream_id, None)
        self._send(0, M.FILE_DONE, to=offer["from"], file_id=rx.file_id, ok=ok)
        self._emit(
            "file_done", file_id=rx.file_id, ok=ok, name=offer["name"], direction="recv"
        )

    async def _send_file_data(self, file_id):
        try:
            sender, to = self._outgoing[file_id]
            cid = self.conn_id
            sid = self.endpoint.open_stream(cid)
            await self._send_data(
                cid,
                sid,
                M.encode(M.FILE_BEGIN, to=to, file_id=file_id, size=sender.size),
            )
            for chunk in sender.chunks():
                if self.conn_id != cid:
                    raise ConnectionError("connection changed")
                stage = await self._send_data(cid, sid, chunk)
                self._emit(
                    "file_progress",
                    file_id=file_id,
                    direction="send",
                    done=sender.sent,
                    total=sender.size,
                    stage=stage,
                )
        except (ValueError, KeyError, OSError, ConnectionError, RuntimeError) as exc:
            self._emit("error", reason=f"file send failed: {exc}")
            item = self._outgoing.get(file_id)
            if item and self.conn_id is not None:
                self._notify_file_error(item[1], file_id, "send failed")
            self._drop_file(file_id)
        finally:
            self._send_tasks.pop(file_id, None)

    async def _send_data(self, cid, sid, data):
        # Adapter for A5's documented SendReceipt API. Current main exercises only v1.
        send = getattr(self.endpoint, "send_message", None)
        if send:
            receipt = await send(cid, sid, data)
            await receipt.wait_acked()
            return "acked_to_server"
        if not self.endpoint.send(cid, sid, data):
            raise ConnectionError("transport did not enqueue message")
        await asyncio.sleep(0)  # event-loop fairness only, NOT backpressure
        return "queued"

    def _notify_file_error(self, to, fid, reason):
        try:
            self._send(0, M.FILE_ERROR, to=to, file_id=fid, reason=reason)
        except ConnectionError:
            log.debug("unable to notify peer of file failure; local cleanup continues")

    def _drop_file(self, fid):
        task = self._send_tasks.pop(fid, None)
        if task and task is not asyncio.current_task():
            task.cancel()
        rx = self._receivers.pop(fid, None)
        if rx:
            rx.abort()
        self._outgoing.pop(fid, None)
        self._incoming_offers.pop(fid, None)
        self._sending.discard(fid)
        self._recv_streams = {
            sid: x for sid, x in self._recv_streams.items() if x != fid
        }
        path = self._owned_paths.pop(fid, None)
        if path:
            try:
                Path(path).unlink(missing_ok=True)
                Path(path).parent.rmdir()
            except OSError:
                pass

    def _cleanup_files(self, reason):
        for fid in (
            set(self._outgoing) | set(self._receivers) | set(self._incoming_offers)
        ):
            self._drop_file(fid)
            self._emit("error", reason=f"file {fid}: {reason}; no automatic resume")

    def _on_disconnect(self, conn_id, reason):
        if conn_id != self.conn_id:
            return
        self.conn_id = self.username = None
        self.users = []
        self._cleanup_files(reason)
        if self._login_future and not self._login_future.done():
            self._login_future.set_result((False, reason))
        self._emit("users", users=[])
        self._emit("status", state="disconnected", reason=reason)
        self._schedule_reconnect()

    def state(self):
        stats = None
        if self.conn_id is not None and hasattr(self.endpoint, "get_stats"):
            value = self.endpoint.get_stats(self.conn_id)
            from dataclasses import asdict, is_dataclass

            stats = asdict(value) if is_dataclass(value) else value
        return dict(
            username=self.username,
            users=sorted(self.users),
            connected=self.conn_id is not None,
            authenticated=self.username is not None,
            transport_stats=stats,
            bootstrap_available=load_bootstrap() is not None,
            progress_mode="transport_receipt"
            if callable(getattr(self.endpoint, "send_message", None))
            else "queued",
        )

    def _send(self, stream_id, msg_type, **fields):
        if self.conn_id is None or not self.endpoint.send(
            self.conn_id, stream_id, M.encode(msg_type, **fields)
        ):
            raise ConnectionError("not connected or transport queue full")

    def _emit(self, event, **fields):
        if self.events.full():
            self.events.get_nowait()  # bounded UI history; network/file correctness does not depend on UI
        self.events.put_nowait({"event": event, **fields})
