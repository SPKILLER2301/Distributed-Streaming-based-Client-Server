"""Public transport contracts and fixed v2 resource profile (A5)."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from common import constants as C


class MessageTooLarge(ValueError):
    pass


class StreamLimitError(RuntimeError):
    pass


class StreamCancelled(ConnectionError):
    pass


class ConnectionClosed(ConnectionError):
    pass


class TransportProtocolError(ConnectionError):
    pass


class QueueFull(BufferError):
    pass


@dataclass(frozen=True)
class FlowLimits:
    stream_bytes: int = 256 * 1024
    connection_bytes: int = 2 * 1024 * 1024
    reserve_bytes: int = 128 * 1024
    control_message: int = 64 * 1024
    file_message: int = 16 * 1024
    stream_messages: int = 1024
    connection_messages: int = 4096
    stream_packets: int = 1024
    connection_packets: int = 4096
    reserve_packets: int = 128
    active_streams: int = 32
    lifetime_streams: int = 1024

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 for v in vars(self).values()):
            raise ValueError("flow limits must be positive integers")
        if not self.reserve_bytes < self.connection_bytes or not self.reserve_packets < self.connection_packets:
            raise ValueError("reserve must be smaller than connection capacity")
        if max(self.file_message, self.control_message) > min(self.stream_bytes, self.connection_bytes):
            raise ValueError("one message must fit in initial byte credit")
        if self.file_message > self.connection_bytes - self.reserve_bytes:
            raise ValueError("file message must fit in file credit")
        packets = max(1, (max(self.file_message, self.control_message) + C.MAX_PAYLOAD - 1) // C.MAX_PAYLOAD)
        if packets > min(self.stream_packets, self.connection_packets - self.reserve_packets):
            raise ValueError("one message must fit in initial packet credit")
        if self.active_streams < 2 or self.lifetime_streams < self.active_streams:
            raise ValueError("invalid stream caps")

    def message_limit(self, sid: int) -> int:
        return self.control_message if sid < C.STREAM_FILE_BASE else self.file_message


class SendReceipt:
    """Payload ACK progress; waiting/cancelling a waiter never cancels DATA."""
    def __init__(self, message_id: int, payload_bytes: int, packets: int):
        self.message_id, self.payload_bytes = message_id, payload_bytes
        self.acked_bytes = 0
        self._remaining = packets
        self._done = asyncio.Event()
        self._error: Exception | None = None

    def _ack(self, size: int):
        self.acked_bytes += size
        self._remaining -= 1
        if self._remaining == 0:
            self._done.set()

    def _fail(self, error: Exception):
        self._error = error
        self._done.set()

    async def wait_acked(self) -> None:
        await self._done.wait()
        if self._error:
            raise self._error


@dataclass(frozen=True)
class ReceivedMessage:
    conn_id: int
    stream_id: int
    data: bytes
    delivery_id: int
    _packets: int = field(repr=False)
    _owner: object = field(repr=False, compare=False)


@dataclass(frozen=True)
class StreamStats:
    stream_id: int
    pending_bytes: int
    unacked_bytes: int
    recv_buffered_bytes: int
    queued_messages: int
    recv_buffered_packets: int
    cancelled: bool


@dataclass(frozen=True)
class TransportStats:
    state: str
    reason: str | None
    algorithm: str
    srtt_seconds: float | None
    rto_seconds: float
    cwnd_packets: float
    inflight_packets: int
    pending_bytes: int
    unacked_bytes: int
    wire_cache_bytes: int
    recv_buffered_bytes: int
    recv_buffered_packets: int
    queued_messages: int
    acked_payload_bytes: int
    retransmitted_packets: int
    timeout_count: int
    streams: tuple[StreamStats, ...]
