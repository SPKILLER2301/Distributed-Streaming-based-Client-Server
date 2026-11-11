"""A5 flow accounting. Counters are cumulative; retransmits spend no credit."""
from __future__ import annotations

import asyncio
import struct

from common import constants as C
from common.packet import Packet
from .flow import (ConnectionClosed, MessageTooLarge, QueueFull, ReceivedMessage,
                   SendReceipt, StreamCancelled, StreamLimitError, StreamStats,
                   TransportStats)
from .stream import RecvStream, SendStream

CREDIT = struct.Struct("!6Q")  # stream/connection/file bytes, then packet slots
RESET = struct.Struct("!QQB")  # final unique bytes, final packet count, reason


class FlowControl:
    def _init_flow(self, limits, on_delivery, on_stream_reset):
        self.limits = limits
        self._on_delivery = on_delivery
        self._on_stream_reset = on_stream_reset
        self._space = asyncio.Event()
        self._owner = object()
        self._message_id = self._delivery_id = 0
        self._deliveries = {}
        self._tx_bytes = self._tx_packets = self._tx_file_bytes = self._tx_file_packets = 0
        self._rx_bytes = self._rx_packets = self._rx_file_bytes = self._rx_file_packets = 0
        self._consumed_bytes = self._consumed_packets = 0
        self._file_consumed_bytes = self._file_consumed_packets = 0
        self._peer_conn_bytes = limits.connection_bytes
        self._peer_conn_packets = limits.connection_packets
        self._peer_file_bytes = limits.connection_bytes - limits.reserve_bytes
        self._peer_file_packets = limits.connection_packets - limits.reserve_packets
        self._probes = {}
        self._reset_out = {}
        self._stop_requests = {}
        self._priority_run = 0
        self._priority_cursor = self._file_cursor = 0
        self.reason = None
        self._recv_peak = self._send_peak = 0

    def _check_sid(self, sid):
        if type(sid) is not int or not 0 <= sid <= 65535:
            raise ValueError("invalid stream id")

    def _check_stream_cap(self, streams, sid, receiving=False):
        active = [s for s in streams.values() if
                  (s.reset_final is None if receiving else not s.cancelled)]
        if len(streams) >= self.limits.lifetime_streams or len(active) >= self.limits.active_streams:
            raise StreamLimitError("stream limit reached")
        if sid >= 2 and sum(s.stream_id >= 2 for s in active) >= self.limits.active_streams - 2:
            raise StreamLimitError("reserved control/chat stream slots")

    def _send_stream(self, sid):
        self._check_sid(sid)
        stream = self.send_streams.get(sid)
        if stream is None:
            self._check_stream_cap(self.send_streams, sid)
            stream = self.send_streams[sid] = SendStream(sid)
            stream.peer_credit = (self.limits.stream_bytes, self.limits.stream_packets)
        if stream.cancelled:
            raise StreamCancelled("stream cancelled")
        # Direct recovery unit tests may construct a SendStream explicitly.
        if stream.peer_credit is None:
            stream.peer_credit = (self.limits.stream_bytes, self.limits.stream_packets)
        return stream

    def _recv_stream(self, sid):
        self._check_sid(sid)
        if sid not in self.recv_streams:
            self._check_stream_cap(self.recv_streams, sid, receiving=True)
            self.recv_streams[sid] = RecvStream(sid)
        return self.recv_streams[sid]

    def _enqueue(self, sid, data):
        if self.closed:
            raise ConnectionClosed(self.reason or "closed")
        if not isinstance(data, bytes):
            raise TypeError("data must be immutable bytes")
        if len(data) > self.limits.message_limit(sid):
            raise MessageTooLarge("message exceeds stream limit")
        stream = self._send_stream(sid)
        all_streams = list(self.send_streams.values())
        total = sum(s.queued_bytes for s in all_streams)
        messages = sum(s.queued_messages for s in all_streams)
        files = [s for s in all_streams if s.stream_id >= 2]
        file_slots = self.limits.connection_messages - min(
            self.limits.reserve_packets, max(1, self.limits.connection_messages // 4))
        if (stream.queued_bytes + len(data) > self.limits.stream_bytes or
                total + len(data) > self.limits.connection_bytes or
                stream.queued_messages >= self.limits.stream_messages or
                messages >= self.limits.connection_messages or
                (sid >= 2 and (sum(s.queued_bytes for s in files) + len(data) >
                               self.limits.connection_bytes - self.limits.reserve_bytes or
                               sum(s.queued_messages for s in files) >= file_slots))):
            raise QueueFull("send queue full")
        start = stream.next_seq
        try:
            stream.enqueue(data)
        except OverflowError as exc:
            self._finish("sequence exhausted")
            raise ConnectionClosed("sequence exhausted") from exc
        receipt = SendReceipt(self._message_id, len(data), stream.next_seq - start)
        self._message_id += 1
        for seq in range(start, stream.next_seq):
            stream.receipts[seq] = receipt
        stream.queued_bytes += len(data)
        stream.queued_messages += 1
        self._send_peak = max(self._send_peak, total + len(data))
        self._pump()
        return receipt

    async def enqueue_message(self, sid, data):
        while True:
            self._space.clear()
            try:
                return self._enqueue(sid, data)
            except QueueFull:
                await self._space.wait()

    def _ack_payload(self, stream, inf):
        receipt = stream.receipts.pop(inf.pkt.seq, None)
        if receipt is not None:
            stream.queued_bytes -= len(inf.pkt.payload)
            receipt._ack(len(inf.pkt.payload))
            if receipt._remaining == 0:
                stream.queued_messages -= 1
        self._space.set()

    def _credit_allowed(self, stream):
        if not stream.can_send() or stream.cancelled:
            return False
        if stream.peer_credit is None:
            stream.peer_credit = (self.limits.stream_bytes, self.limits.stream_packets)
        size = len(stream.pending[0].payload)
        allowed = (stream.sent_bytes + size <= stream.peer_credit[0] and
                   stream.highest_sent + 2 <= stream.peer_credit[1] and
                   self._tx_bytes + size <= self._peer_conn_bytes and
                   self._tx_packets + 1 <= self._peer_conn_packets)
        if stream.stream_id >= 2:
            allowed = allowed and (self._tx_file_bytes + size <= self._peer_file_bytes and
                                   self._tx_file_packets + 1 <= self._peer_file_packets)
        return allowed

    def _record_sent(self, stream, pkt):
        size = len(pkt.payload)
        stream.sent_bytes += size
        self._tx_bytes += size
        self._tx_packets += 1
        if stream.stream_id >= 2:
            self._tx_file_bytes += size
            self._tx_file_packets += 1

    def _select_stream(self):
        eligible = [s for s in self.send_streams.values() if self._credit_allowed(s)]
        priority = sorted((s for s in eligible if s.stream_id < 2), key=lambda s: s.stream_id)
        files = sorted((s for s in eligible if s.stream_id >= 2), key=lambda s: s.stream_id)
        if priority and (self._priority_run < 4 or not files):
            chosen = next((s for s in priority if s.stream_id >= self._priority_cursor), priority[0])
            self._priority_cursor = chosen.stream_id + 1
            self._priority_run = min(4, self._priority_run + 1)
            return chosen
        if files:
            chosen = next((s for s in files if s.stream_id >= self._file_cursor), files[0])
            self._file_cursor = chosen.stream_id + 1
            self._priority_run = 0
            return chosen
        return None

    def _credit_values(self, stream):
        l = self.limits
        return (l.stream_bytes + stream.consumed_bytes,
                l.connection_bytes + self._consumed_bytes,
                l.connection_bytes - l.reserve_bytes + self._file_consumed_bytes,
                l.stream_packets + stream.consumed_packets,
                l.connection_packets + self._consumed_packets,
                l.connection_packets - l.reserve_packets + self._file_consumed_packets)

    def _advertise_credit(self, sid):
        stream = self.recv_streams[sid]
        if stream.reset_final is None:
            self._transmit(Packet(C.PKT_CREDIT, stream_id=sid,
                                  payload=CREDIT.pack(*self._credit_values(stream))))

    def _on_credit(self, pkt):
        sb, cb, fb, sp, cp, fp = CREDIT.unpack(pkt.payload)
        self._peer_conn_bytes = max(self._peer_conn_bytes, cb)
        self._peer_conn_packets = max(self._peer_conn_packets, cp)
        self._peer_file_bytes = max(self._peer_file_bytes, fb)
        self._peer_file_packets = max(self._peer_file_packets, fp)
        stream = self.send_streams.get(pkt.stream_id)
        if stream is not None and not stream.cancelled:
            old = stream.peer_credit or (self.limits.stream_bytes, self.limits.stream_packets)
            stream.peer_credit = max(old[0], sb), max(old[1], sp)
        self._pump()

    def _receive_data(self, pkt, plain):
        stream = self._recv_stream(pkt.stream_id)
        if stream.reset_final is not None or stream.stopping:
            return
        duplicate = pkt.seq < stream.expected or pkt.seq in stream.buffer
        if duplicate or pkt.seq >= stream.expected + C.MAX_WINDOW:
            ack, sack = stream.ack_info()
            self._transmit(Packet(C.PKT_ACK, stream_id=pkt.stream_id, ack=ack, sack=sack))
            return
        sb, cb, fb, sp, cp, fp = self._credit_values(stream)
        size = len(plain)
        if (stream.received_bytes + size > sb or stream.received_packets + 1 > sp or
                self._rx_bytes + size > cb or self._rx_packets + 1 > cp or
                (pkt.stream_id >= 2 and (self._rx_file_bytes + size > fb or self._rx_file_packets + 1 > fp))):
            # No ACK for data that cannot be retained. Compliant peers probe
            # for credit; this also bounds memory under invalid peer input.
            self.stats["flow_blocked"] += 1
            return
        stream.received_bytes += size
        stream.received_packets += 1
        self._rx_bytes += size
        self._rx_packets += 1
        if pkt.stream_id >= 2:
            self._rx_file_bytes += size
            self._rx_file_packets += 1
        self._recv_peak = max(self._recv_peak, self._rx_bytes - self._consumed_bytes)
        messages = stream.on_packet(pkt.seq, pkt.flags, plain, self.limits.message_limit(pkt.stream_id))
        ack, sack = stream.ack_info()
        self._transmit(Packet(C.PKT_ACK, stream_id=pkt.stream_id, ack=ack, sack=sack))
        for data, packets in zip(messages, stream.message_packet_counts):
            if self.closed:
                break
            msg = ReceivedMessage(self.conn_id, pkt.stream_id, data,
                                  self._delivery_id, packets, self._owner)
            self._delivery_id += 1
            self._deliveries[msg.delivery_id] = msg
            if self._on_delivery is not None:
                self._on_delivery(msg)
            else:
                self._on_message(self.conn_id, pkt.stream_id, data)
                self.consume(msg)

    def consume(self, message):
        if message._owner is not self._owner or message.conn_id != self.conn_id:
            raise ValueError("delivery token belongs to another connection")
        saved = self._deliveries.get(message.delivery_id)
        if saved is None:
            return
        if saved is not message:
            raise ValueError("invalid delivery token")
        del self._deliveries[message.delivery_id]
        stream = self.recv_streams[message.stream_id]
        stream.consumed_bytes += len(message.data)
        stream.consumed_packets += message._packets
        self._consumed_bytes += len(message.data)
        self._consumed_packets += message._packets
        if message.stream_id >= 2:
            self._file_consumed_bytes += len(message.data)
            self._file_consumed_packets += message._packets
        self._advertise_credit(message.stream_id)

    def _flow_tick(self, now):
        for sid, stream in list(self.send_streams.items()):
            if stream.pending and not stream.cancelled and not self._credit_allowed(stream):
                deadline, interval = self._probes.setdefault(sid, (now + self.rtt.rto, self.rtt.rto))
                if now >= deadline:
                    self._transmit(Packet(C.PKT_CREDIT_PROBE, stream_id=sid))
                    interval = min(interval * 2, C.MAX_RTO)
                    self._probes[sid] = now + interval, interval
            else:
                self._probes.pop(sid, None)
        for sid, reset in list(self._reset_out.items()):
            if now >= reset["deadline"]:
                if reset["tries"] >= C.MAX_RETRIES:
                    self._finish("too many retries")
                    return
                reset["tries"] += 1
                reset["interval"] = min(reset["interval"] * 2, C.MAX_RTO)
                reset["deadline"] = now + reset["interval"]
                self._transmit(Packet(C.PKT_RESET_STREAM, stream_id=sid, payload=reset["body"]))
        for sid, (deadline, tries) in list(self._stop_requests.items()):
            if now >= deadline:
                if tries >= C.MAX_RETRIES:
                    self._finish("too many retries")
                    return
                self._transmit(Packet(C.PKT_STOP_STREAM, stream_id=sid, payload=b"\x01"))
                self._stop_requests[sid] = now + self.rtt.rto, tries + 1

    def _discard_send(self, sid, reason="cancelled"):
        stream = self.send_streams[sid]
        for receipt in set(stream.receipts.values()):
            receipt._fail(StreamCancelled(reason))
        stream.receipts.clear()
        stream.pending.clear()
        stream.unacked.clear()
        stream.queued_bytes = stream.queued_messages = 0
        stream.cancelled = True
        self._probes.pop(sid, None)
        for key in list(self._retransmit_pending):
            if key[0] == sid:
                del self._retransmit_pending[key]
        self._maybe_end_recovery()
        self._sync_cc_usage(self._clock())
        self._space.set()

    def begin_cancel(self, sid, reason="cancelled"):
        if sid < 2:
            raise ValueError("control/chat streams cannot be cancelled")
        if self.closed:
            raise ConnectionClosed(self.reason or "closed")
        if sid in self._reset_out:
            return self._reset_out[sid]
        stream = self.send_streams.get(sid)
        if stream is not None and stream.cancelled:
            return None
        stream = self._send_stream(sid)
        body = RESET.pack(stream.sent_bytes, stream.highest_sent + 1, 1)
        self._discard_send(sid, reason)
        reset = {"body": body, "done": asyncio.Event(), "error": None,
                 "deadline": self._clock() + self.rtt.rto,
                 "interval": self.rtt.rto, "tries": 0}
        self._reset_out[sid] = reset
        self._transmit(Packet(C.PKT_RESET_STREAM, stream_id=sid, payload=body))
        return reset

    async def cancel_stream(self, sid, reason="cancelled"):
        reset = self.begin_cancel(sid, reason)
        if reset is not None:
            await reset["done"].wait()
            if reset["error"]:
                raise reset["error"]

    def request_stop(self, sid):
        if sid < 2:
            self.close("receive handler error")
            return
        stream = self._recv_stream(sid)
        stream.stopping = True
        self._stop_requests[sid] = self._clock() + self.rtt.rto, 0
        self._transmit(Packet(C.PKT_STOP_STREAM, stream_id=sid, payload=b"\x01"))

    def _on_reset(self, pkt):
        if pkt.stream_id < 2:
            raise ValueError("invalid reset stream")
        final_bytes, final_packets, reason = RESET.unpack(pkt.payload)
        stream = self._recv_stream(pkt.stream_id)
        if stream.reset_final is not None:
            if stream.reset_final != pkt.payload:
                raise ValueError("reset final size changed")
        else:
            sb, cb, fb, sp, cp, fp = self._credit_values(stream)
            db, dp = final_bytes - stream.received_bytes, final_packets - stream.received_packets
            if (db < 0 or dp < 0 or final_bytes > sb or final_packets > sp or
                    self._rx_bytes + db > cb or self._rx_file_bytes + db > fb or
                    self._rx_packets + dp > cp or self._rx_file_packets + dp > fp or
                    final_bytes > final_packets * C.MAX_PAYLOAD):
                raise ValueError("invalid reset final size")
            self._rx_bytes += db
            self._rx_file_bytes += db
            self._rx_packets += dp
            self._rx_file_packets += dp
            released_bytes = final_bytes - stream.consumed_bytes
            released_packets = final_packets - stream.consumed_packets
            self._consumed_bytes += released_bytes
            self._file_consumed_bytes += released_bytes
            self._consumed_packets += released_packets
            self._file_consumed_packets += released_packets
            stream.received_bytes = stream.consumed_bytes = final_bytes
            stream.received_packets = stream.consumed_packets = final_packets
            stream.buffer.clear()
            stream._fragments.clear()
            stream.fragment_bytes = stream.fragment_packets = 0
            stream.reset_final = pkt.payload
            for key, msg in list(self._deliveries.items()):
                if msg.stream_id == pkt.stream_id:
                    del self._deliveries[key]
            self._stop_requests.pop(pkt.stream_id, None)
            if self._on_stream_reset is not None:
                self._on_stream_reset(self.conn_id, pkt.stream_id)
        self._transmit(Packet(C.PKT_RESET_ACK, stream_id=pkt.stream_id, payload=pkt.payload))
        # Reset ACK echoes final size; CREDIT on a live stream also advertises
        # freed connection capacity. A sender without a live stream will probe.
        for sid, rs in self.recv_streams.items():
            if rs.reset_final is None:
                self._advertise_credit(sid)

    def _on_reset_ack(self, pkt):
        reset = self._reset_out.get(pkt.stream_id)
        if reset is not None and reset["body"] == pkt.payload:
            reset["done"].set()
            del self._reset_out[pkt.stream_id]

    def _close_flow(self, reason):
        self.reason = reason
        error = ConnectionClosed(reason)
        for stream in self.send_streams.values():
            for receipt in set(stream.receipts.values()):
                receipt._fail(error)
            stream.receipts.clear()
        for reset in self._reset_out.values():
            reset["error"] = error
            reset["done"].set()
        self._reset_out.clear()
        self._deliveries.clear()
        self._consumed_bytes, self._consumed_packets = self._rx_bytes, self._rx_packets
        self._file_consumed_bytes, self._file_consumed_packets = self._rx_file_bytes, self._rx_file_packets
        self._probes.clear()
        self._stop_requests.clear()
        self._space.set()

    def get_stats(self):
        streams = []
        for sid in sorted(set(self.send_streams) | set(self.recv_streams)):
            ss, rs = self.send_streams.get(sid), self.recv_streams.get(sid)
            streams.append(StreamStats(sid,
                sum(len(p.payload) for p in ss.pending) if ss else 0,
                sum(len(i.pkt.payload) for i in ss.unacked.values()) if ss else 0,
                rs.received_bytes - rs.consumed_bytes if rs else 0,
                ss.queued_messages if ss else 0,
                rs.received_packets - rs.consumed_packets if rs else 0,
                bool((ss and ss.cancelled) or (rs and rs.reset_final is not None))))
        return TransportStats("closed" if self.closed else "connected", self.reason, self.cc.algorithm,
            self.rtt.srtt, self.rtt.rto, self.cc.cwnd, self._inflight_total(),
            sum(s.pending_bytes for s in streams), sum(s.unacked_bytes for s in streams),
            sum(len(i.wire_data) for s in self.send_streams.values() for i in s.unacked.values()),
            self._rx_bytes - self._consumed_bytes, self._rx_packets - self._consumed_packets,
            sum(s.queued_messages for s in streams), self.stats["acked_payload_bytes"],
            self.stats["retransmitted"], self.stats["timeout_count"], tuple(streams))
