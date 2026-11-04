"""
Mã hóa payload.                                           [Người phụ trách: A]  (Bước 8)

Thiết kế:
  1. Handshake: mỗi bên sinh cặp khóa X25519 tạm thời, trao đổi khóa công khai trong HELLO / HELLO_REPLY.
  2. Từ bí mật chung dùng HKDF-SHA256 sinh 64 byte: 32 byte khóa client->server, 32 byte khóa server->client.
     => mỗi chiều một khóa riêng nên nonce không bao giờ trùng giữa hai chiều.
  3. Mỗi gói DATA mã hóa bằng AES-256-GCM:
        nonce = stream_id(2B) || seq(4B) || 6 byte 0     (duy nhất trong 1 khóa + 1 chiều)
        AAD   = header của gói  -> sửa header cũng bị phát hiện
     Gửi lại cùng (stream_id, seq) dùng cùng nonce nhưng cùng plaintext nên an toàn.

Hạn chế đã biết (nên ghi vào báo cáo): trao đổi khóa chưa xác thực => chưa chống man-in-the-middle chủ động.
Có thể nâng cấp bằng cách ký/MAC bằng mật khẩu dùng chung (PSK).
"""
from __future__ import annotations

import struct

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


class CryptoError(Exception):
    """Giải mã thất bại (sai khóa, gói bị sửa) -> bỏ gói."""


class SessionCipher:
    def __init__(self, send_key: bytes, recv_key: bytes):
        self._send = AESGCM(send_key)
        self._recv = AESGCM(recv_key)

    @staticmethod
    def _nonce(stream_id: int, seq: int) -> bytes:
        return struct.pack("!HI", stream_id, seq) + b"\x00" * 6

    def encrypt(self, stream_id: int, seq: int, aad: bytes, plaintext: bytes) -> bytes:
        """Trả về ciphertext + 16 byte tag."""
        return self._send.encrypt(self._nonce(stream_id, seq), plaintext, aad)

    def decrypt(self, stream_id: int, seq: int, aad: bytes, ciphertext: bytes) -> bytes:
        try:
            return self._recv.decrypt(self._nonce(stream_id, seq), ciphertext, aad)
        except InvalidTag as exc:
            raise CryptoError("tag không hợp lệ") from exc


class KeyExchange:
    """Một lần trao đổi khóa X25519."""

    def __init__(self) -> None:
        self._priv = X25519PrivateKey.generate()

    @property
    def public_bytes(self) -> bytes:
        return self._priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)

    def derive_cipher(self, peer_public: bytes, is_client: bool) -> SessionCipher:
        try:
            shared = self._priv.exchange(X25519PublicKey.from_public_bytes(peer_public))
        except ValueError as exc:
            raise CryptoError("khóa công khai không hợp lệ") from exc
        okm = HKDF(algorithm=hashes.SHA256(), length=64, salt=None, info=b"collab-app v1").derive(shared)
        c2s, s2c = okm[:32], okm[32:]
        return SessionCipher(c2s, s2c) if is_client else SessionCipher(s2c, c2s)
