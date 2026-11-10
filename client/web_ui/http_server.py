"""Local HTTP/1.1 UI with bounded streaming uploads and one SSE tab per daemon."""

from __future__ import annotations
import asyncio
import json
import logging
import mimetypes
import shutil
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote
from common import messages as M
from client.file_transfer import FileSender

log = logging.getLogger(__name__)
STATIC_DIR = Path(__file__).parent / "static"
MAX_BODY = M.MAX_FILE_SIZE


class HTTPError(ValueError):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


class WebUI:
    def __init__(self, client, port, host="127.0.0.1"):
        self.client, self.port, self.host = client, port, host
        self.outbox = tempfile.mkdtemp(prefix="collab_outbox_")
        self._server = None
        self._handlers = set()
        self._uploads = asyncio.Semaphore(2)
        self._sse_active = False

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle, self.host, self.port, limit=16384
        )
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        tasks = list(self._handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        shutil.rmtree(self.outbox, ignore_errors=True)

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._handlers.add(task)
        try:
            line = await asyncio.wait_for(reader.readline(), 10)
            if not line:
                return
            if len(line) > 8192:
                raise HTTPError(400, "request line too long")
            parts = line.decode("latin-1").strip().split()
            if len(parts) != 3 or parts[2] != "HTTP/1.1":
                raise HTTPError(400, "expected HTTP/1.1 request")
            method, target, _ = parts
            headers, total = {}, 0
            for _ in range(64):
                h = await asyncio.wait_for(reader.readline(), 10)
                total += len(h)
                if total > 16384 or not h:
                    raise HTTPError(400, "invalid headers")
                if h == b"\r\n":
                    break
                key, sep, value = h.decode("latin-1").partition(":")
                key = key.strip().lower()
                if not sep or key in headers:
                    raise HTTPError(400, "malformed or duplicate header")
                headers[key] = value.strip()
            else:
                raise HTTPError(400, "too many headers")
            allowed = {f"127.0.0.1:{self.port}", f"localhost:{self.port}"}
            if headers.get("host") not in allowed:
                raise HTTPError(403, "local Host required")
            origin = headers.get("origin")
            if origin and origin not in {f"http://{h}" for h in allowed}:
                raise HTTPError(403, "cross-origin request rejected")
            if "transfer-encoding" in headers:
                raise HTTPError(400, "use Content-Length")
            try:
                length = int(headers.get("content-length", "0"))
            except ValueError:
                raise HTTPError(400, "invalid Content-Length")
            if length < 0:
                raise HTTPError(400, "negative Content-Length")
            url = urlparse(target)
            if method == "POST" and url.path == "/api/file":
                if length > MAX_BODY:
                    raise HTTPError(413, "file too large")
                await self._upload(reader, writer, parse_qs(url.query), length)
            else:
                if length > 65536:
                    raise HTTPError(413, "JSON body too large")
                body = (
                    await asyncio.wait_for(reader.readexactly(length), 30)
                    if length
                    else b""
                )
                await self._route(method, unquote(url.path), body, writer)
        except HTTPError as exc:
            await self._json(writer, {"ok": False, "reason": exc.reason}, exc.status)
        except (ValueError, KeyError, TypeError) as exc:
            await self._json(writer, {"ok": False, "reason": str(exc)}, 400)
        except asyncio.TimeoutError:
            await self._json(writer, {"ok": False, "reason": "request timed out"}, 408)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        except Exception:
            log.exception("HTTP handler failed")
            await self._json(writer, {"ok": False, "reason": "internal error"}, 500)
        finally:
            self._handlers.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _respond(self, writer, status, body, ctype):
        reason = {
            200: "OK",
            400: "Bad Request",
            403: "Forbidden",
            404: "Not Found",
            408: "Request Timeout",
            409: "Conflict",
            413: "Payload Too Large",
            429: "Too Many Requests",
            500: "Internal Server Error",
        }.get(status, "Error")
        writer.write(
            (
                f"HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                "X-Content-Type-Options: nosniff\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n"
            ).encode()
            + body
        )
        await asyncio.wait_for(writer.drain(), 10)

    async def _json(self, writer, obj, status=200):
        await self._respond(
            writer, status, json.dumps(obj).encode(), "application/json"
        )

    def _data(self, body):
        data = json.loads(body or b"{}")
        if not isinstance(data, dict):
            raise ValueError("JSON object required")
        return data

    async def _route(self, method, path, body, writer):
        c = self.client
        if method == "GET" and path == "/api/events":
            if self._sse_active:
                raise HTTPError(409, "one UI tab per daemon; use another --ui-port")
            await self._sse(writer)
        elif method == "GET" and path == "/api/state":
            await self._json(writer, c.state())
        elif method == "POST" and path == "/api/login":
            data = self._data(body)
            ok, reason = await c.login(
                data.get("username", ""), data.get("password", "")
            )
            await self._json(writer, {"ok": ok, "reason": reason})
        elif method == "POST" and path == "/api/logout":
            await c.logout()
            await self._json(writer, {"ok": True})
        elif method == "POST" and path == "/api/chat":
            data = self._data(body)
            c.send_chat(data.get("text", ""), data.get("to") or None)
            await self._json(writer, {"ok": True})
        elif method == "POST" and path in ("/api/file/accept", "/api/file/reject"):
            data = self._data(body)
            (c.accept_file if path.endswith("accept") else c.reject_file)(
                data.get("file_id", "")
            )
            await self._json(writer, {"ok": True})
        elif method == "GET":
            rel = (
                "index.html"
                if path == "/"
                else path.removeprefix("/static/").lstrip("/")
            )
            full = (STATIC_DIR / rel).resolve()
            if STATIC_DIR.resolve() not in full.parents or not full.is_file():
                raise HTTPError(404, "not found")
            mime = mimetypes.guess_type(full.name)[0] or "application/octet-stream"
            await self._respond(
                writer, 200, full.read_bytes(), mime + "; charset=utf-8"
            )
        else:
            raise HTTPError(404, "not found")

    async def _upload(self, reader, writer, query, length):
        c = self.client
        if not c.username or c.conn_id is None:
            raise HTTPError(409, "login required")
        if self._uploads.locked() or len(c._outgoing) >= M.MAX_TRANSFERS:
            raise HTTPError(429, "upload limit reached")
        to, name = query.get("to", [""])[0], query.get("name", ["file"])[0]
        if (
            not to
            or to == c.username
            or name in ("", ".", "..")
            or any(x in name for x in ("/", "\\", "\x00"))
            or len(name) > 255
        ):
            raise HTTPError(400, "invalid recipient or filename")
        folder = Path(tempfile.mkdtemp(dir=self.outbox))
        path = folder / name
        cid = c.conn_id
        committed = False
        try:
            async with self._uploads:
                with path.open("xb") as f:
                    remaining = length
                    deadline = asyncio.get_running_loop().time() + 300
                    while remaining:
                        timeout = min(30, deadline - asyncio.get_running_loop().time())
                        chunk = await asyncio.wait_for(
                            reader.read(min(65536, remaining)), max(0, timeout)
                        )
                        if not chunk:
                            raise asyncio.IncompleteReadError(b"", remaining)
                        f.write(chunk)
                        remaining -= len(chunk)
                sender = await asyncio.to_thread(FileSender, str(path))
                if c.conn_id != cid or not c.username:
                    raise HTTPError(409, "connection changed during upload; retry")
                fid = c.offer_sender(sender, to, delete_after=True)
                committed = True
                await self._json(writer, {"ok": True, "file_id": fid})
        finally:
            if not committed:
                shutil.rmtree(folder, ignore_errors=True)

    async def _sse(self, writer):
        self._sse_active = True
        try:
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
            )
            initial = [
                {"event": "users", "users": self.client.users},
                {
                    "event": "status",
                    "state": "connected"
                    if self.client.conn_id is not None
                    else "disconnected",
                },
            ]
            for fid, offer in self.client._incoming_offers.items():
                if fid not in self.client._receivers:
                    initial.append(
                        {
                            "event": "file_offer",
                            **{
                                k: offer[k] for k in ("file_id", "from", "name", "size")
                            },
                        }
                    )
            for event in initial:
                writer.write(f"data: {json.dumps(event)}\n\n".encode())
            await writer.drain()
            while True:
                try:
                    event = await asyncio.wait_for(self.client.events.get(), 5)
                    writer.write(f"data: {json.dumps(event)}\n\n".encode())
                except asyncio.TimeoutError:
                    writer.write(b": heartbeat\n\n")
                await asyncio.wait_for(writer.drain(), 10)
        finally:
            self._sse_active = False
