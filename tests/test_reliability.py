"""Kiểm thử tầng transport: mã hóa, handshake, stream, và (TODO) mất gói.   [A]"""
import asyncio
import json
from contextlib import asynccontextmanager

import pytest

from common import constants as C
from tests.lossy_socket import LossyLink
from transport.crypto import CryptoError, KeyExchange
from transport.endpoint import Endpoint


def test_crypto_roundtrip_and_tamper():
    a, b = KeyExchange(), KeyExchange()
    ca = a.derive_cipher(b.public_bytes, is_client=True)
    cb = b.derive_cipher(a.public_bytes, is_client=False)
    ct = ca.encrypt(1, 5, b"hdr", b"secret")
    assert cb.decrypt(1, 5, b"hdr", ct) == b"secret"
    with pytest.raises(CryptoError):
        cb.decrypt(1, 5, b"hdr-sua", ct)                      # sửa header -> phát hiện
    with pytest.raises(CryptoError):
        cb.decrypt(1, 6, b"hdr", ct)                          # sai seq/nonce


@asynccontextmanager
async def _pair(link_factory=None):
    """Dựng server + client trên localhost. Trả (server, client, conn_client, conn_server, nhận_ở_server, nhận_ở_client)."""
    got_s, got_c = [], []
    conns = {}
    server = Endpoint(is_server=True)
    server.set_handlers(on_connect=lambda cid, addr: conns.setdefault("s", cid),
                        on_receive=lambda cid, sid, d: got_s.append((sid, d)))
    client = Endpoint(is_server=False)
    client.set_handlers(on_receive=lambda cid, sid, d: got_c.append((sid, d)))
    try:
        await server.start(("127.0.0.1", 0))
        await client.start(("127.0.0.1", 0))
        if link_factory:
            server.link, client.link = link_factory(), link_factory()
        cid = await client.connect(("127.0.0.1", server.local_addr[1]))
        yield server, client, cid, conns["s"], got_s, got_c
    finally:
        for endpoint in (client, server):
            if endpoint.link is not None:
                endpoint.link.close()
            await endpoint.stop()


async def _wait(cond, timeout=5.0):
    end = asyncio.get_running_loop().time() + timeout
    while not cond():
        assert asyncio.get_running_loop().time() < end, "hết thời gian chờ"
        await asyncio.sleep(0.01)


def test_handshake_and_messages_both_directions():
    async def run():
        async with _pair() as (server, client, cid, sid_conn, got_s, got_c):
            client.send(cid, C.STREAM_CHAT, b"hello")
            server.send(sid_conn, C.STREAM_CONTROL, b"world")
            await _wait(lambda: got_s and got_c)
            assert got_s == [(C.STREAM_CHAT, b"hello")] and got_c == [(C.STREAM_CONTROL, b"world")]
    asyncio.run(run())


def test_large_message_fragmentation_and_window():
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            blob = bytes(range(256)) * 2000  # ~512 KB, vượt cwnd khởi đầu
            for offset in range(0, len(blob), 8192):
                await client.send_message(cid, 5, blob[offset:offset + 8192])
            await _wait(lambda: sum(len(data) for _, data in got_s) == len(blob), timeout=10)
            assert [sid for sid, _ in got_s] == [5] * ((len(blob) + 8191) // 8192)
            assert b"".join(data for _, data in got_s) == blob
    asyncio.run(run())


def test_messages_keep_order_within_stream():
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            for i in range(200):
                client.send(cid, C.STREAM_CHAT, str(i).encode())
            await _wait(lambda: len(got_s) == 200)
            assert [d for _, d in got_s] == [str(i).encode() for i in range(200)]
    asyncio.run(run())


# ------------------------------------------------------------------ mất gói / đảo thứ tự; TODO(A): thêm HoL + ngắt kết nối bên dưới
def test_delivery_with_10_percent_loss():
    async def run():
        async with _pair(lambda: LossyLink(loss=0.10, seed=1)) as (server, client, cid, _, got_s, _):
            blob = b"x" * 200_000
            for offset in range(0, len(blob), 8192):
                await client.send_message(cid, 5, blob[offset:offset + 8192])
            await _wait(lambda: sum(len(data) for _, data in got_s) == len(blob), timeout=20)
            conn = client.connections[cid]
            await _wait(lambda: not conn.send_streams[5].unacked, timeout=20)
            assert b"".join(data for _, data in got_s) == blob
            print(json.dumps({"case": "loss_10_percent", "seed_each_direction": 1,
                              "loss_each_direction": 0.1, "payload_bytes": len(blob),
                              "stats": conn.stats, "cwnd_packets": conn.cc.cwnd,
                              "pending_packets": len(conn.send_streams[5].pending)}))
    asyncio.run(run())


def test_reorder_and_duplicate():
    async def run():
        async with _pair(lambda: LossyLink(jitter=0.03, duplicate=0.1, seed=2)) as (
                server, client, cid, _, got_s, _):
            for i in range(100):
                client.send(cid, C.STREAM_CHAT, str(i).encode())
            await _wait(lambda: len(got_s) == 100, timeout=10)
            await _wait(lambda: not client.connections[cid].send_streams[1].unacked, timeout=10)
            assert [d for _, d in got_s] == [str(i).encode() for i in range(100)]
    asyncio.run(run())


@pytest.mark.parametrize("case", ["data", "ack", "tail", "retransmit"])
def test_targeted_loss_over_real_udp(case):
    async def run():
        async with _pair() as (server, client, cid, _, got_s, _):
            client.link = LossyLink(record=True, drop_rule=lambda pkt, n:
                pkt.ptype == C.PKT_DATA and (
                    (case == "data" and pkt.seq == 1 and n == 1) or
                    (case == "tail" and pkt.seq == 7 and n == 1) or
                    (case == "retransmit" and pkt.seq == 1 and n <= 2)))
            server.link = LossyLink(record=True, drop_rule=lambda pkt, n:
                case == "ack" and pkt.ptype == C.PKT_ACK and n == 1)
            expected = [str(i).encode() for i in range(8)]
            for payload in expected:
                assert client.send(cid, 5, payload)
            conn = client.connections[cid]
            await _wait(lambda: len(got_s) == len(expected), timeout=5)
            await _wait(lambda: not conn.send_streams[5].unacked, timeout=5)
            assert got_s == [(5, payload) for payload in expected]
            assert conn.stats["acked_payload_bytes"] == sum(map(len, expected))
            if case in ("data", "retransmit"):
                assert conn.stats["fast_retransmitted"] == 1
            if case in ("ack", "tail", "retransmit"):
                assert conn.stats["timeout_retransmitted"] >= 1
            packets = [event for event in client.link.events
                       if event["packet"].ptype == C.PKT_DATA]
            for seq in range(8):
                assert len({event["wire"] for event in packets
                            if event["packet"].seq == seq}) == 1
            print(json.dumps({"case": "targeted_" + case, "stats": conn.stats}))
    asyncio.run(run())


@pytest.mark.skip(reason="TODO(A): chứng minh không có HoL blocking")
def test_no_head_of_line_blocking():
    """Gợi ý: gửi file lớn trên stream 5 qua link mất 20% gói, đồng thời gửi tin nhắn nhỏ trên stream 1.
    Tin nhắn trên stream 1 phải đến sớm hơn nhiều so với lúc file hoàn tất."""


@pytest.mark.skip(reason="TODO(A): cần đóng kết nối khi quá MAX_RETRIES / PEER_TIMEOUT")
def test_peer_disconnect_detected():
    """Gợi ý: đặt link loss=1.0 ở một phía, chờ on_disconnect được gọi ở phía kia."""
