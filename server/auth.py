"""Salted PBKDF2 credentials. Demo passwords are documented in README."""

from __future__ import annotations
import hashlib
import hmac
import json
import secrets
from pathlib import Path

ITERATIONS = 260_000


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256", password.encode(), bytes.fromhex(salt), ITERATIONS
    )
    return f"pbkdf2_sha256${ITERATIONS}${salt}${digest.hex()}"


class Auth:
    def __init__(self, users_file: str | Path) -> None:
        try:
            self._users = json.loads(Path(users_file).read_text(encoding="utf-8"))
            if not isinstance(self._users, dict):
                self._users = {}
        except (OSError, ValueError):
            self._users = {}

    def verify(self, username: str, password: str) -> bool:
        if not isinstance(username, str) or not isinstance(password, str):
            return False
        if len(username) > 64 or len(password) > 1024:
            return False
        expected = self._users.get(username, "")
        try:
            algorithm, rounds, salt, digest = expected.split("$")
            if algorithm != "pbkdf2_sha256" or not 100_000 <= int(rounds) <= 1_000_000:
                return False
            actual = hashlib.pbkdf2_hmac(
                "sha256", password.encode(), bytes.fromhex(salt), int(rounds)
            ).hex()
            return hmac.compare_digest(digest, actual)
        except (ValueError, TypeError, AttributeError):
            return False
