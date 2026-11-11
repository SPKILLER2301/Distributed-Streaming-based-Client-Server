"""
Message tầng ứng dụng (JSON) đặt trong payload của transport.   [Người phụ trách: B]  (Bước 1, 3, 5)

Mỗi message là một dict có khóa "type". Xem bảng đầy đủ trong docs/protocol.md.
Serialization dùng JSON (dễ debug). Chunk file KHÔNG dùng JSON – gửi bytes thô (xem client/client.py).

Cách dùng:
    data = messages.encode(messages.MSG_BROADCAST, text="hello")
    msg  = messages.decode(data)     # raise ValueError nếu không hợp lệ
"""

from __future__ import annotations

import json

# --- Xác thực & danh sách user ---
AUTH = "AUTH"  # C->S  {username, password}
AUTH_OK = "AUTH_OK"  # S->C  {username, users}
AUTH_FAIL = "AUTH_FAIL"  # S->C  {reason}
LOGOUT = "LOGOUT"  # C->S
USER_LIST = "USER_LIST"  # S->C  {users}
USER_JOINED = "USER_JOINED"  # S->C  {username}
USER_LEFT = "USER_LEFT"  # S->C  {username}
# --- Chat ---
MSG_BROADCAST = "MSG_BROADCAST"  # C->S {text}      S->C {from, text}
MSG_PRIVATE = "MSG_PRIVATE"  # C->S {to, text}  S->C {from, text}
# --- File ---
FILE_OFFER = "FILE_OFFER"  # {to|from, file_id, name, size, sha256}
FILE_ACCEPT = "FILE_ACCEPT"  # {to|from, file_id}
FILE_REJECT = "FILE_REJECT"  # {to|from, file_id}
FILE_BEGIN = "FILE_BEGIN"  # {to|from, file_id, size}  message đầu tiên trên stream file
FILE_DONE = "FILE_DONE"  # {to|from, file_id, ok}
FILE_ERROR = "FILE_ERROR"  # {file_id, reason}
# --- Lỗi chung ---
ERROR = "ERROR"  # S->C {reason}


def encode(msg_type: str, **fields) -> bytes:
    """Dựng message JSON -> bytes."""
    return json.dumps({"type": msg_type, **fields}, separators=(",", ":")).encode(
        "utf-8"
    )


def decode(data: bytes) -> dict:
    """bytes -> dict. Raise ValueError nếu không phải JSON object có khóa 'type'."""
    try:
        msg = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message không hợp lệ: {exc}") from exc
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        raise ValueError("message thiếu trường 'type'")
    return msg


# Application limits; transport limits remain owned by A.
MAX_FILE_SIZE = 200 * 1024 * 1024
MAX_TRANSFERS = 16


def validate_client(msg: dict, stream_id: int) -> None:
    """Validate untrusted client fields before routing or changing application state."""
    import re

    t = msg["type"]
    control = {
        AUTH,
        LOGOUT,
        FILE_OFFER,
        FILE_ACCEPT,
        FILE_REJECT,
        FILE_DONE,
        FILE_ERROR,
    }
    if t in control and stream_id != 0:
        raise ValueError("control message requires stream 0")
    if t in {MSG_BROADCAST, MSG_PRIVATE} and stream_id != 1:
        raise ValueError("chat requires stream 1")

    def string(key, maximum=64):
        value = msg.get(key)
        if not isinstance(value, str) or not value or len(value) > maximum:
            raise ValueError(f"invalid {key}")

    if t == AUTH:
        string("username")
        string("password", 1024)
    if t in {MSG_BROADCAST, MSG_PRIVATE}:
        string("text", 4096)
    if t == MSG_PRIVATE or t in {
        FILE_OFFER,
        FILE_ACCEPT,
        FILE_REJECT,
        FILE_DONE,
        FILE_BEGIN,
        FILE_ERROR,
    }:
        string("to")
    if t in {FILE_OFFER, FILE_ACCEPT, FILE_REJECT, FILE_DONE, FILE_BEGIN, FILE_ERROR}:
        string("file_id")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", msg["file_id"]):
            raise ValueError("invalid file_id")
    if t in {FILE_OFFER, FILE_BEGIN}:
        if type(msg.get("size")) is not int or not 0 <= msg["size"] <= MAX_FILE_SIZE:
            raise ValueError("invalid size")
    if t == FILE_OFFER:
        string("name", 255)
        if msg["name"] in {".", ".."} or any(
            c in msg["name"] for c in ("/", "\\", "\x00")
        ):
            raise ValueError("unsafe filename")
        if not isinstance(msg.get("sha256"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", msg["sha256"]
        ):
            raise ValueError("invalid sha256")
    if t == FILE_DONE and type(msg.get("ok")) is not bool:
        raise ValueError("invalid done result")
