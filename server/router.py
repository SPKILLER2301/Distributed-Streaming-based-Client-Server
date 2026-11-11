"""Application routing and authoritative file state. Owner: B.

Transport v1 send() means enqueue only: this module does not claim backpressure.
"""

from __future__ import annotations
import time
from dataclasses import dataclass, field
from common import constants as C, messages as M


@dataclass
class Transfer:
    sender: str
    receiver: str
    size: int
    state: str = "offered"
    remaining: int = 0
    relay: tuple | None = None
    updated: float = field(default_factory=time.monotonic)


class Router:
    def __init__(self, endpoint, registry):
        self.endpoint, self.registry = endpoint, registry
        self.transfers = {}
        self._relays = {}

    def send_conn(self, conn_id, stream_id, msg_type, **fields):
        return self.endpoint.send(conn_id, stream_id, M.encode(msg_type, **fields))

    def send_user(self, username, stream_id, msg_type, **fields):
        info = self.registry.get_by_name(username)
        return bool(
            info and self.send_conn(info.conn_id, stream_id, msg_type, **fields)
        )

    def broadcast(self, sender, text):
        for user in self.registry.all():
            if user.username != sender:
                self.send_conn(
                    user.conn_id,
                    C.STREAM_CHAT,
                    M.MSG_BROADCAST,
                    **{"from": sender, "text": text},
                )

    def unicast(self, sender, to, text):
        ok = self.send_user(
            to, C.STREAM_CHAT, M.MSG_PRIVATE, **{"from": sender, "text": text}
        )
        if not ok:
            self.send_user(sender, 0, M.ERROR, reason=f"người dùng '{to}' không online")
        return ok

    def announce(self, msg_type, **fields):
        for user in self.registry.all():
            self.send_conn(user.conn_id, 0, msg_type, **fields)

    def _remove(self, file_id):
        transfer = self.transfers.pop(file_id, None)
        if transfer and transfer.relay:
            self._relays.pop(transfer.relay[:2], None)
        return transfer

    def fail(self, file_id, reason):
        transfer = self._remove(file_id)
        if transfer:
            for name in (transfer.sender, transfer.receiver):
                self.send_user(name, 0, M.FILE_ERROR, file_id=file_id, reason=reason)

    def forward_signal(self, sender, msg):
        t, fid, to = msg["type"], msg["file_id"], msg["to"]
        transfer = self.transfers.get(fid)
        if t == M.FILE_OFFER:
            if transfer or sender == to or not self.registry.get_by_name(to):
                raise ValueError("duplicate offer or unavailable recipient")
            for participant in (sender, to):
                if (
                    sum(
                        participant in (x.sender, x.receiver)
                        for x in self.transfers.values()
                    )
                    >= M.MAX_TRANSFERS
                ):
                    raise ValueError("too many active file offers")
            transfer = self.transfers[fid] = Transfer(sender, to, msg["size"])
        else:
            if not transfer:
                raise ValueError("unknown file offer")
            if t == M.FILE_ERROR:
                if (sender, to) not in {
                    (transfer.sender, transfer.receiver),
                    (transfer.receiver, transfer.sender),
                }:
                    raise ValueError("file participant mismatch")
                self.fail(fid, "peer reported file error")
                return
            if (sender, to) != (transfer.receiver, transfer.sender):
                raise ValueError("file participant mismatch")
            if t in (M.FILE_ACCEPT, M.FILE_REJECT):
                if transfer.state != "offered":
                    raise ValueError("offer already answered")
                transfer.state = "accepted"
                if t == M.FILE_REJECT:
                    self._remove(fid)
            elif t == M.FILE_DONE:
                if transfer.state != "await_done":
                    raise ValueError("DONE before relay completed")
                self._remove(fid)
        transfer.updated = time.monotonic()
        fields = {k: v for k, v in msg.items() if k not in {"type", "to", "from"}}
        if not self.send_user(to, 0, t, **{"from": sender, **fields}):
            self.fail(fid, "recipient disconnected")

    def relay_file_stream(self, conn_id, stream_id, data):
        key = (conn_id, stream_id)
        fid = self._relays.get(key)
        if fid is None:
            msg = M.decode(data)
            M.validate_client(msg, stream_id)
            if msg["type"] != M.FILE_BEGIN:
                raise ValueError("file stream requires BEGIN")
            fid = msg["file_id"]
            transfer = self.transfers.get(fid)
            sender = self.registry.get_by_conn(conn_id)
            dest = self.registry.get_by_name(msg["to"])
            if (
                not transfer
                or not sender
                or not dest
                or transfer.state != "accepted"
                or (transfer.sender, transfer.receiver, transfer.size)
                != (sender.username, msg["to"], msg["size"])
            ):
                raise ValueError("BEGIN without matching accepted offer")
            sid = self.endpoint.open_stream(dest.conn_id)
            transfer.relay = (conn_id, stream_id, dest.conn_id, sid)
            transfer.remaining = transfer.size
            transfer.state = "relaying" if transfer.size else "await_done"
            if transfer.size:
                self._relays[key] = fid
            if not self.send_conn(
                dest.conn_id,
                sid,
                M.FILE_BEGIN,
                **{"from": sender.username, "file_id": fid, "size": transfer.size},
            ):
                self.fail(fid, "relay unavailable")
        else:
            transfer = self.transfers[fid]
            if (
                not data
                or len(data) > C.FILE_CHUNK_SIZE
                or len(data) > transfer.remaining
            ):
                self.fail(fid, "chunk exceeds offered size")
                raise ValueError("invalid file chunk")
            _, _, dest_conn, dest_sid = transfer.relay
            if not self.endpoint.send(dest_conn, dest_sid, data):
                self.fail(fid, "relay unavailable")
                return
            transfer.remaining -= len(data)
            if not transfer.remaining:
                transfer.state = "await_done"
                self._relays.pop(key, None)
        transfer.updated = time.monotonic()

    def cleanup_file_relays(self, conn_id):
        user = self.registry.get_by_conn(conn_id)
        if user:
            for fid, transfer in list(self.transfers.items()):
                if user.username in (transfer.sender, transfer.receiver):
                    self.fail(fid, "peer disconnected; file will not resume")

    def expire(self, timeout=120):
        for fid, transfer in list(self.transfers.items()):
            if time.monotonic() - transfer.updated > timeout:
                self.fail(fid, "file transfer timed out")
