"""
Mã hóa payload.                                           [Người phụ trách: A]  (Bước 8)

Thiết kế:
  1. Handshake: mỗi bên sinh cặp khóa X25519 tạm thời, trao đổi khóa công khai trong HELLO / HELLO_REPLY.
  2. Từ bí mật chung dùng HKDF-SHA256 sinh 64 byte: 32 byte khóa client->server, 32 byte khóa server->client.
     => mỗi chiều một khóa riêng; yêu cầu duy nhất áp dụng cho cặp (khóa, nonce).
  3. Mỗi gói DATA mã hóa bằng AES-256-GCM:
        nonce = 0x00 || stream_id(2B) || seq(4B) || 5 byte 0 (wire v2)
        AAD   = header của gói  -> sửa header cũng bị phát hiện
     DATA retransmit dùng cache bytes, không encrypt lại. Control dùng miền
     nonce 0x01 || counter(8B) || 3 byte 0, header+counter làm AAD.

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
from common import constants as C


class CryptoError(Exception):
    """Giải mã thất bại (sai khóa, gói bị sửa) -> bỏ gói."""


class SessionCipher:
    def __init__(self, send_key: bytes, recv_key: bytes):
        self._send = AESGCM(send_key)
        self._recv = AESGCM(recv_key)

    @staticmethod
    def _nonce(stream_id: int, seq: int) -> bytes:
        if type(stream_id) is not int or not 0 <= stream_id < 1 << 16:
            raise ValueError("stream_id must fit uint16")
        if type(seq) is not int or not 0 <= seq < 1 << 32:
            raise ValueError("sequence must fit uint32")
        return b"\x00" + struct.pack("!HI", stream_id, seq) + b"\x00" * 5

    @staticmethod
    def _control_nonce(counter: int) -> bytes:
        if type(counter) is not int or not 0 <= counter < 1 << 64:
            raise ValueError("control counter must fit uint64")
        return b"\x01" + struct.pack("!Q", counter) + b"\x00" * 3

    def encrypt(self, stream_id: int, seq: int, aad: bytes, plaintext: bytes) -> bytes:
        """Trả về ciphertext + 16 byte tag."""
        return self._send.encrypt(self._nonce(stream_id, seq), plaintext, aad)

    def decrypt(self, stream_id: int, seq: int, aad: bytes, ciphertext: bytes) -> bytes:
        try:
            return self._recv.decrypt(self._nonce(stream_id, seq), ciphertext, aad)
        except InvalidTag as exc:
            raise CryptoError("tag không hợp lệ") from exc

    def encrypt_control(self, counter: int, aad: bytes, plaintext: bytes) -> bytes:
        nonce = self._control_nonce(counter)
        return self._send.encrypt(nonce, plaintext, aad)

    def decrypt_control(self, counter: int, aad: bytes, ciphertext: bytes) -> bytes:
        nonce = self._control_nonce(counter)
        try:
            return self._recv.decrypt(nonce, ciphertext, aad)
        except InvalidTag as exc:
            raise CryptoError("invalid control tag") from exc


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
        info = f"collab-app v{C.PROTOCOL_VERSION}".encode("ascii")
        okm = HKDF(algorithm=hashes.SHA256(), length=64, salt=None, info=info).derive(shared)
        c2s, s2c = okm[:32], okm[32:]
        return SessionCipher(c2s, s2c) if is_client else SessionCipher(s2c, c2s)
