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
AUTH = "AUTH"                  # C->S  {username, password}
AUTH_OK = "AUTH_OK"            # S->C  {username, users}
AUTH_FAIL = "AUTH_FAIL"        # S->C  {reason}
LOGOUT = "LOGOUT"              # C->S
USER_LIST = "USER_LIST"        # S->C  {users}
USER_JOINED = "USER_JOINED"    # S->C  {username}
USER_LEFT = "USER_LEFT"        # S->C  {username}
# --- Chat ---
MSG_BROADCAST = "MSG_BROADCAST"  # C->S {text}      S->C {from, text}
MSG_PRIVATE = "MSG_PRIVATE"      # C->S {to, text}  S->C {from, text}
# --- File ---
FILE_OFFER = "FILE_OFFER"      # {to|from, file_id, name, size, sha256}
FILE_ACCEPT = "FILE_ACCEPT"    # {to|from, file_id}
FILE_REJECT = "FILE_REJECT"    # {to|from, file_id}
FILE_BEGIN = "FILE_BEGIN"      # {to|from, file_id, size}  message đầu tiên trên stream file
FILE_DONE = "FILE_DONE"        # {to|from, file_id, ok}
FILE_ERROR = "FILE_ERROR"      # {file_id, reason}
# --- Lỗi chung ---
ERROR = "ERROR"                # S->C {reason}


def encode(msg_type: str, **fields) -> bytes:
    """Dựng message JSON -> bytes."""
    return json.dumps({"type": msg_type, **fields}, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> dict:
    """bytes -> dict. Raise ValueError nếu không phải JSON object có khóa 'type'."""
    try:
        msg = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"message không hợp lệ: {exc}") from exc
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        raise ValueError("message thiếu trường 'type'")
    return msg
