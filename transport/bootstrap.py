"""TCP discovery before the UDP handshake (A2).

One newline-terminated UTF-8 JSON frame per connection, at most 4096 bytes
including the newline. B integrates these APIs into application lifecycle.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from typing import Any

from common import constants as C

Addr = tuple[str, int]
MAX_FRAME_BYTES = 4096
DEFAULT_TIMEOUT = 3.0
_CLOSE_TIMEOUT = 0.5
log = logging.getLogger(__name__)


class BootstrapError(ConnectionError):
    """Bootstrap failed; reason is a stable code suitable for application use."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _check_version(version: int) -> None:
    if type(version) is not int or not 0 <= version <= 255:
        raise ValueError("version must be an integer in 0..255")


def _check_port(port: int, *, allow_zero: bool = False) -> None:
    if type(port) is not int or not (0 if allow_zero else 1) <= port <= 65535:
        raise ValueError("invalid port")


def _check_timeout(timeout: float) -> None:
    if isinstance(timeout, bool) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")


def _encode(message: dict[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8") + b"\n"


async def _read_frame(reader: asyncio.StreamReader) -> dict[str, Any]:
    try:
        raw = await reader.readuntil(b"\n")
    except asyncio.LimitOverrunError as exc:
        raise BootstrapError("frame_too_large") from exc
    except asyncio.IncompleteReadError as exc:
        raise BootstrapError("unexpected_eof") from exc
    if len(raw) > MAX_FRAME_BYTES:
        raise BootstrapError("frame_too_large")
    try:
        message = json.loads(raw.decode("utf-8"))
    except (ValueError, RecursionError) as exc:
        raise BootstrapError("invalid_frame") from exc
    if not isinstance(message, dict):
        raise BootstrapError("invalid_frame")
    return message


async def _close_writer(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), _CLOSE_TIMEOUT)
    except (OSError, asyncio.TimeoutError):
        # Abort a stalled close rather than leave a socket behind.
        writer.transport.abort()


class BootstrapServer:
    """Serve UDP discovery; start/stop own the TCP listener and accepted tasks.

    tcp_port=0 is supported for tests. udp_port must be the actual bound UDP
    port; the server never creates or modifies a UDP Endpoint.
    """

    def __init__(self, host: str, tcp_port: int, udp_port: int,
                 version: int = C.PROTOCOL_VERSION, *,
                 timeout: float = DEFAULT_TIMEOUT) -> None:
        _check_port(tcp_port, allow_zero=True)
        _check_port(udp_port)
        _check_version(version)
        _check_timeout(timeout)
        self.host, self.tcp_port, self.udp_port = host, tcp_port, udp_port
        self.version, self.timeout = version, timeout
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task] = set()
        self._writers: set[asyncio.StreamWriter] = set()

    @property
    def local_addr(self) -> Addr:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("bootstrap server is not running")
        addr = self._server.sockets[0].getsockname()
        return addr[0], addr[1]

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("bootstrap server is already running")
        self._server = await asyncio.start_server(
            self._accept, self.host, self.tcp_port, limit=MAX_FRAME_BYTES - 1)

    def _accept(self, reader: asyncio.StreamReader,
                writer: asyncio.StreamWriter) -> None:
        # Register immediately: stop may cancel a task before its first step,
        # in which case the coroutine's finally block has not been entered.
        self._writers.add(writer)
        task = asyncio.create_task(self._handle(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        try:
            await asyncio.wait_for(self._exchange(reader, writer), self.timeout)
        except (BootstrapError, OSError, asyncio.TimeoutError) as exc:
            # A malformed request gets no success ACK. Other clients continue.
            log.debug("bootstrap request rejected: %s", exc)
        finally:
            await _close_writer(writer)
            self._writers.discard(writer)

    async def _exchange(self, reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> None:
        request = await _read_frame(reader)
        if request.get("type") != "BOOTSTRAP":
            raise BootstrapError("invalid_request")
        if type(request.get("version")) is not int or request["version"] != self.version:
            raise BootstrapError("version_mismatch")
        writer.write(_encode({"type": "BOOTSTRAP_ACK", "version": self.version,
                              "udp_port": self.udp_port}))
        await writer.drain()

    async def stop(self) -> None:
        """Close the listener, cancel incomplete exchanges and await cleanup."""
        server, self._server = self._server, None
        if server is not None:
            server.close()
        tasks = list(self._tasks)
        writers = list(self._writers)
        for writer in writers:
            writer.close()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if writers:
            await asyncio.gather(*(_close_writer(writer) for writer in writers))
        self._writers.difference_update(writers)
        self._tasks.difference_update(tasks)
        # Some Python versions wait for accepted transports as well as the
        # listener; close those transports before awaiting the server.
        if server is not None:
            await server.wait_closed()


async def bootstrap_connect(server_addr: Addr, *,
                            version: int = C.PROTOCOL_VERSION,
                            timeout: float = DEFAULT_TIMEOUT) -> Addr:
    """Return the connected TCP peer's IP and its advertised UDP port.

    The timeout covers TCP connect, request drain and the complete response.
    The writer is closed on success, failure or cancellation. No UDP fallback.
    """
    _check_version(version)
    _check_port(server_addr[1])
    _check_timeout(timeout)
    writer: asyncio.StreamWriter | None = None

    async def exchange() -> Addr:
        nonlocal writer
        reader, writer = await asyncio.open_connection(
            *server_addr, limit=MAX_FRAME_BYTES - 1)
        peer = writer.get_extra_info("peername")
        writer.write(_encode({"type": "BOOTSTRAP", "version": version}))
        await writer.drain()
        response = await _read_frame(reader)
        if response.get("type") != "BOOTSTRAP_ACK":
            raise BootstrapError("invalid_response")
        if type(response.get("version")) is not int or response["version"] != version:
            raise BootstrapError("version_mismatch")
        port = response.get("udp_port")
        if type(port) is not int or not 1 <= port <= 65535:
            raise BootstrapError("invalid_response")
        return peer[0], port

    try:
        return await asyncio.wait_for(exchange(), timeout)
    except BootstrapError:
        raise
    except asyncio.TimeoutError as exc:
        raise BootstrapError("timeout") from exc
    except OSError as exc:
        raise BootstrapError("connection_failed") from exc
    finally:
        if writer is not None:
            await _close_writer(writer)
