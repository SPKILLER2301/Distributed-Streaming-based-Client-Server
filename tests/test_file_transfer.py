"""Kiểm thử chia chunk / ghép file / kiểm hash (client/file_transfer.py).   [A]"""

import os

import pytest

from client.file_transfer import FileReceiver, FileSender


def _make_file(tmp_path, size):
    p = tmp_path / "src.bin"
    p.write_bytes(os.urandom(size))
    return str(p)


@pytest.mark.parametrize("size", [0, 1, 8192, 8193, 1_000_003])
def test_roundtrip_various_sizes(tmp_path, size):
    path = _make_file(tmp_path, size)
    s = FileSender(path)
    r = FileReceiver(s.file_id, s.name, s.size, s.sha256, str(tmp_path / "out"))
    for chunk in s.chunks():
        r.write(chunk)
    assert r.finish() is True
    assert open(r.path, "rb").read() == open(path, "rb").read()


def test_corruption_detected(tmp_path):
    path = _make_file(tmp_path, 20000)
    s = FileSender(path)
    r = FileReceiver(s.file_id, s.name, s.size, s.sha256, str(tmp_path / "out"))
    chunks = list(s.chunks())
    chunks[1] = b"\x00" * len(chunks[1])  # làm hỏng 1 chunk
    for c in chunks:
        r.write(c)
    assert r.finish() is False
    assert not os.path.exists(r.path)  # file hỏng bị xóa


def test_path_traversal_name_is_sanitized(tmp_path):
    r = FileReceiver("id", "../../evil.txt", 0, "x", str(tmp_path / "out"))
    assert os.path.dirname(os.path.abspath(r.path)) == str(tmp_path / "out")
    r.abort()


def test_file_over_lossy_network(tmp_path):
    import asyncio
    from tests.app_helpers import session, wait_until, event
    from tests.lossy_socket import LossyLink

    async def run():
        async with session(tmp_path) as (server, (a, b)):
            # Each endpoint's outbound DATA/ACK path is impaired: both relay hops.
            server.endpoint.link = LossyLink(
                loss=0.10, delay=0.001, jitter=0.003, duplicate=0.02, seed=31
            )
            a.endpoint.link = LossyLink(
                loss=0.10, delay=0.001, jitter=0.003, duplicate=0.02, seed=32
            )
            b.endpoint.link = LossyLink(
                loss=0.10, delay=0.001, jitter=0.003, duplicate=0.02, seed=33
            )
            source = tmp_path / "loss.bin"
            payload = bytes(range(256)) * 128
            source.write_bytes(payload)
            fid = a.send_file(str(source), "bob")
            await wait_until(lambda: fid in b._incoming_offers, 30)
            b.accept_file(fid)
            received = await event(
                b, "file_done", lambda ev: ev["file_id"] == fid, timeout=60
            )
            sent = await event(
                a, "file_done", lambda ev: ev["file_id"] == fid, timeout=60
            )
            assert received["ok"] and sent["ok"]
            assert (tmp_path / "1/loss.bin").read_bytes() == payload
            assert not server.router.transfers and not server.router._relays
            assert (
                sum(ep.link.dropped for ep in (server.endpoint, a.endpoint, b.endpoint))
                > 0
            )

    asyncio.run(run())
