"""Verify B's ordering against A's interface, not a substitute for real A2 tests."""

import asyncio
from client.client import CollabClient
from server.server import CollabServer
from tests.app_helpers import USERS


def test_client_bootstrap_precedes_udp_and_login(monkeypatch):
    order = []

    async def bootstrap(addr, **kwargs):
        order.append("tcp_ack")
        return ("127.0.0.1", 1234)

    monkeypatch.setattr("client.client.load_bootstrap", lambda: (None, bootstrap))

    async def run():
        c = CollabClient(("127.0.0.1", 9000))

        async def connect(addr):
            assert addr == ("127.0.0.1", 1234)
            order.append("udp")
            return 7

        c.endpoint.connect = connect
        await c._connect()
        assert c.conn_id == 7 and order == ["tcp_ack", "udp"]

    asyncio.run(run())


def test_server_starts_stops_bootstrap(monkeypatch):
    order = []

    class Bootstrap:
        def __init__(self, host, tcp_port, udp_port, version):
            assert udp_port > 0

        async def start(self):
            order.append("start")

        async def stop(self):
            order.append("stop")

    monkeypatch.setattr("server.server.load_bootstrap", lambda: (Bootstrap, None))

    async def run():
        server = CollabServer("127.0.0.1", 0, USERS)
        await server.start()
        await server.stop()
        assert order == ["start", "stop"]

    asyncio.run(run())


def test_tcp_failure_never_falls_back_to_udp(monkeypatch):
    async def bootstrap(*args, **kwargs):
        raise ConnectionError("invalid bootstrap ACK")

    monkeypatch.setattr("client.client.load_bootstrap", lambda: (None, bootstrap))

    async def run():
        c = CollabClient(("127.0.0.1", 9000))
        import pytest

        with pytest.raises(ConnectionError):
            await c._connect()
        assert c.conn_id is None

    asyncio.run(run())
