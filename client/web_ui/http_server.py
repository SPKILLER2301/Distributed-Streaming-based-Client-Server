"""
HTTP server cục bộ cho giao diện web (chỉ JavaScript phía trình duyệt).   [Người phụ trách: B]  (Bước 9)

Tự viết bộ phân tích HTTP/1.1 tối giản (không Flask/Django/WebSocket). Trình duyệt nói chuyện với
client của bạn qua HTTP trên 127.0.0.1; client nói chuyện với server qua UDP tin cậy.

Các route:
    GET  /                      -> static/index.html (và /static/*)
    GET  /api/events            -> Server-Sent Events: đẩy mọi event từ client.events xuống trình duyệt
    POST /api/login             {username, password}
    POST /api/chat              {text, to?}
    POST /api/file?to=&name=    body = bytes thô của file (trình duyệt đọc bằng FileReader / fetch body)
    POST /api/file/accept       {file_id}     |   POST /api/file/reject {file_id}

LƯU Ý quy định: asyncio.start_server dùng socket TCP cục bộ chỉ để phục vụ trình duyệt.
Hãy hỏi giảng viên xem có chấp nhận không; nếu không, thay bằng module `socket` + `selectors` thủ công.

TODO(B): trả thêm trạng thái mạng (RTT, số gói gửi lại từ endpoint.connections[...].stats) qua /api/state
         để UI hiển thị "network status" như đề yêu cầu; giới hạn kích thước body (hiện 200 MB).
"""
from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import os
import tempfile
from pathlib import Path
from typing import Dict, Tuple
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).parent / "static"
MAX_BODY = 200 * 1024 * 1024


class WebUI:
    def __init__(self, client, port: int, host: str = "127.0.0.1") -> None:
        self.client, self.port, self.host = client, port, host
        self.outbox = tempfile.mkdtemp(prefix="collab_outbox_")

    async def start(self) -> None:
        await asyncio.start_server(self._handle, self.host, self.port)

    # ---------------------------------------------------------------- HTTP tối giản
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            method, target, _ = line.decode("latin-1").split(" ", 2)
            headers: Dict[str, str] = {}
            while True:
                h = (await reader.readline()).decode("latin-1").strip()
                if not h:
                    break
                k, _, v = h.partition(":")
                headers[k.strip().lower()] = v.strip()
            length = int(headers.get("content-length", "0"))
            if length > MAX_BODY:
                await self._respond(writer, 413, b"too large", "text/plain")
                return
            body = await reader.readexactly(length) if length else b""
            url = urlparse(target)
            await self._route(method, url.path, parse_qs(url.query), body, writer)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception:
            log.exception("lỗi HTTP")
            try:
                await self._respond(writer, 500, b"internal error", "text/plain")
            except Exception:
                pass
        finally:
            try:
                writer.close()
            except Exception:
                pass

    async def _respond(self, writer, status: int, body: bytes, ctype: str) -> None:
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 413: "Payload Too Large",
                  500: "Internal Server Error"}.get(status, "OK")
        head = (f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
        writer.write(head + body)
        await writer.drain()

    async def _json(self, writer, obj, status: int = 200) -> None:
        await self._respond(writer, status, json.dumps(obj).encode(), "application/json")

    # ---------------------------------------------------------------- định tuyến
    async def _route(self, method: str, path: str, query, body: bytes, writer) -> None:
        c = self.client
        if method == "GET" and path == "/api/events":
            await self._sse(writer)
        elif method == "GET" and path == "/api/state":
            await self._json(writer, {"username": c.username, "users": sorted(c.users),
                                      "connected": c.conn_id is not None})
        elif method == "POST" and path == "/api/login":
            data = json.loads(body or b"{}")
            ok, reason = await c.login(data.get("username", ""), data.get("password", ""))
            await self._json(writer, {"ok": ok, "reason": reason})
        elif method == "POST" and path == "/api/chat":
            data = json.loads(body or b"{}")
            c.send_chat(str(data.get("text", "")), data.get("to") or None)
            await self._json(writer, {"ok": True})
        elif method == "POST" and path == "/api/file":
            to = query.get("to", [""])[0]
            name = os.path.basename(query.get("name", ["file"])[0]) or "file"
            tmp = os.path.join(self.outbox, name)
            with open(tmp, "wb") as f:
                f.write(body)
            await self._json(writer, {"ok": True, "file_id": c.send_file(tmp, to)})
        elif method == "POST" and path in ("/api/file/accept", "/api/file/reject"):
            data = json.loads(body or b"{}")
            (c.accept_file if path.endswith("accept") else c.reject_file)(data.get("file_id", ""))
            await self._json(writer, {"ok": True})
        elif method == "GET":
            await self._static(writer, path)
        else:
            await self._respond(writer, 404, b"not found", "text/plain")

    async def _static(self, writer, path: str) -> None:
        rel = "index.html" if path in ("/", "") else path.removeprefix("/static/").lstrip("/")
        full = (STATIC_DIR / rel).resolve()
        if STATIC_DIR.resolve() not in full.parents or not full.is_file():   # chặn ../
            await self._respond(writer, 404, b"not found", "text/plain")
            return
        ctype = mimetypes.guess_type(full.name)[0] or "application/octet-stream"
        await self._respond(writer, 200, full.read_bytes(), ctype + ("; charset=utf-8" if ctype.startswith("text") or "javascript" in ctype else ""))

    async def _sse(self, writer) -> None:
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                     b"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n")
        await writer.drain()
        # gửi trạng thái hiện tại cho trang vừa mở/tải lại
        writer.write(f"data: {json.dumps({'event': 'users', 'users': sorted(self.client.users)})}\n\n".encode())
        while True:
            event = await self.client.events.get()
            writer.write(f"data: {json.dumps(event)}\n\n".encode())
            await writer.drain()            # trình duyệt đóng tab -> ném ConnectionError -> _handle dọn dẹp
