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
    chunks[1] = b"\x00" * len(chunks[1])                     # làm hỏng 1 chunk
    for c in chunks:
        r.write(c)
    assert r.finish() is False
    assert not os.path.exists(r.path)                         # file hỏng bị xóa


def test_path_traversal_name_is_sanitized(tmp_path):
    r = FileReceiver("id", "../../evil.txt", 0, "x", str(tmp_path / "out"))
    assert os.path.dirname(os.path.abspath(r.path)) == str(tmp_path / "out")
    r.abort()


@pytest.mark.skip(reason="TODO(A): gửi file qua mạng có mất gói (cần gửi lại ở transport)")
def test_file_over_lossy_network():
    """Gợi ý: dùng _pair(lambda: LossyLink(loss=0.1)) trong test_reliability, gửi chunk, so sánh SHA-256."""
