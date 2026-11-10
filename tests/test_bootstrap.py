"""A2: real TCP framing, validation, deadlines, cleanup and UDP handoff."""
import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from client.client import CollabClient
from common import constants as C
from server.server import CollabServer
from transport.bootstrap import BootstrapError, BootstrapServer, bootstrap_connect


def frame(**fields):
    return json.dumps(fields, separators=(",", ":")).encode() + b"\n"


@asynccontextmanager
async def bootstrap_server(**kwargs):
    server = BootstrapServer("127.0.0.1", 0, 9000, **kwargs)
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@asynccontextmanager
async def fake_server(handler):
    """Own every accepted task/writer, including assertion failure paths."""
    tasks, writers = set(), set()

    async def run(reader, writer):
        writers.add(writer)
        try:
            await handler(reader, writer)
        finally:
            writer.close()
            await writer.wait_closed()
            writers.discard(writer)

    def accept(reader, writer):
        task = asyncio.create_task(run(reader, writer))
        tasks.add(task)

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        yield server.sockets[0].getsockname()[:2]
    finally:
        server.close()
        await server.wait_closed()
        for writer in list(writers):
            writer.close()
        for task in tasks:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result


def test_bootstrap_ack_and_concurrent_clients():
    async def run():
        async with bootstrap_server() as server:
            results = await asyncio.gather(*(
                bootstrap_connect(server.local_addr) for _ in range(20)))
            assert results == [("127.0.0.1", 9000)] * 20
    asyncio.run(run())


def test_server_accepts_fragmented_request_and_closes_after_ack():
    async def run():
        async with bootstrap_server() as server:
            reader, writer = await asyncio.open_connection(*server.local_addr)
            try:
                writer.write(b'{"type":"BOOT')
                await writer.drain()
                await asyncio.sleep(0)
                writer.write(b'STRAP","version":' + str(C.PROTOCOL_VERSION).encode() + b'}\n')
                await writer.drain()
                response = json.loads(await asyncio.wait_for(reader.readline(), 1))
                assert response == {"type": "BOOTSTRAP_ACK", "version": C.PROTOCOL_VERSION,
                                    "udp_port": 9000}
                assert await asyncio.wait_for(reader.read(), 1) == b""
            finally:
                writer.close()
                await writer.wait_closed()
    asyncio.run(run())


def test_client_waits_for_entire_fragmented_response_and_closes():
    async def run():
        peer_closed = asyncio.Event()

        async def handler(reader, writer):
            request = json.loads(await reader.readline())
            assert request == {"type": "BOOTSTRAP", "version": C.PROTOCOL_VERSION}
            writer.write(b'{"type":"BOOTSTRAP_ACK",')
            await writer.drain()
            # Ensure client cannot finish from just the first TCP fragment.
            await asyncio.sleep(0.02)
            writer.write(frame(version=C.PROTOCOL_VERSION, udp_port=9011)[1:])
            await writer.drain()
            assert await asyncio.wait_for(reader.read(), 1) == b""
            peer_closed.set()

        async with fake_server(handler) as addr:
            assert await bootstrap_connect(addr) == ("127.0.0.1", 9011)
            await asyncio.wait_for(peer_closed.wait(), 1)
    asyncio.run(run())


@pytest.mark.parametrize("request_bytes", [
    frame(type="BOOTSTRAP", version=99),
    frame(type="BOOTSTRAP", version=True),
    frame(type="BOOTSTRAP", version="1"),
    frame(type="BOOTSTRAP"),
    frame(type="OTHER", version=C.PROTOCOL_VERSION),
    b"{broken}\n", b"[]\n", b"\xff\n", b"\n",
    b"x" * 4096 + b"\n", b"x" * 4097,
])
def test_server_rejects_bad_request_and_still_serves_next_client(request_bytes):
    async def run():
        async with bootstrap_server() as server:
            reader, writer = await asyncio.open_connection(*server.local_addr)
            try:
                writer.write(request_bytes)
                await writer.drain()
                assert await asyncio.wait_for(reader.read(), 1) == b""
            finally:
                writer.close()
                await writer.wait_closed()
            assert await bootstrap_connect(server.local_addr) == ("127.0.0.1", 9000)
    asyncio.run(run())


def test_exact_frame_limit_is_accepted():
    async def run():
        request = frame(type="BOOTSTRAP", version=C.PROTOCOL_VERSION)
        request = request[:-1] + b" " * (4096 - len(request)) + b"\n"
        assert len(request) == 4096
        async with bootstrap_server() as server:
            reader, writer = await asyncio.open_connection(*server.local_addr)
            try:
                writer.write(request)
                await writer.drain()
                assert json.loads(await asyncio.wait_for(reader.readline(), 1))["type"] == "BOOTSTRAP_ACK"
            finally:
                writer.close()
                await writer.wait_closed()
    asyncio.run(run())


@pytest.mark.parametrize("response, reason", [
    (frame(type="BOOTSTRAP_ACK", version=99, udp_port=9000), "version_mismatch"),
    (frame(type="BOOTSTRAP_ACK", version=True, udp_port=9000), "version_mismatch"),
    (frame(type="OTHER", version=C.PROTOCOL_VERSION, udp_port=9000), "invalid_response"),
    (frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION, udp_port=0), "invalid_response"),
    (frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION, udp_port=65536), "invalid_response"),
    (frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION, udp_port=True), "invalid_response"),
    (frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION, udp_port="9000"), "invalid_response"),
    (frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION), "invalid_response"),
    (b"{bad}\n", "invalid_frame"), (b"[]\n", "invalid_frame"),
    (b"\xff\n", "invalid_frame"), (b"{}", "unexpected_eof"),
    (b"", "unexpected_eof"), (b"x" * 4097, "frame_too_large"),
])
def test_client_rejects_invalid_response(response, reason):
    async def run():
        async def handler(reader, writer):
            await reader.readline()
            writer.write(response)
            await writer.drain()

        async with fake_server(handler) as addr:
            with pytest.raises(BootstrapError) as error:
                await bootstrap_connect(addr)
            assert error.value.reason == reason
    asyncio.run(run())


def test_client_uses_tcp_peer_address_instead_of_response_host():
    async def run():
        async def handler(reader, writer):
            await reader.readline()
            writer.write(frame(type="BOOTSTRAP_ACK", version=C.PROTOCOL_VERSION,
                               udp_port=9000, host="203.0.113.123"))
            await writer.drain()

        async with fake_server(handler) as addr:
            assert await bootstrap_connect(("localhost", addr[1])) == ("127.0.0.1", 9000)
    asyncio.run(run())


@pytest.mark.parametrize("cancel", [False, True])
def test_client_timeout_or_cancellation_closes_socket(cancel):
    async def run():
        requested, closed = asyncio.Event(), asyncio.Event()

        async def handler(reader, writer):
            await reader.readline()
            requested.set()
            assert await asyncio.wait_for(reader.read(), 1) == b""
            closed.set()

        async with fake_server(handler) as addr:
            task = asyncio.create_task(bootstrap_connect(addr, timeout=0.2 if not cancel else 3))
            try:
                await asyncio.wait_for(requested.wait(), 1)
                if cancel:
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                else:
                    with pytest.raises(BootstrapError, match="timeout"):
                        await task
                await asyncio.wait_for(closed.wait(), 1)
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_client_connect_phase_has_deadline(monkeypatch):
    async def pending_connect(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio, "open_connection", pending_connect)

    async def run():
        with pytest.raises(BootstrapError, match="timeout"):
            await bootstrap_connect(("127.0.0.1", 9000), timeout=0.02)
    asyncio.run(run())


@pytest.mark.parametrize("stop", [False, True])
def test_server_timeout_and_stop_close_incomplete_requests(stop):
    async def run():
        async with bootstrap_server(timeout=10 if stop else 0.05) as server:
            addr = server.local_addr
            reader, writer = await asyncio.open_connection(*addr)
            try:
                writer.write(b'{"type":')
                await writer.drain()
                if stop:
                    await asyncio.wait_for(server.stop(), 1)
                    assert not server._tasks
                    assert not server._writers
                    with pytest.raises(OSError):
                        await asyncio.open_connection(*addr)
                assert await asyncio.wait_for(reader.read(), 1) == b""
            finally:
                writer.close()
                await writer.wait_closed()
            if not stop:
                assert await bootstrap_connect(addr) == ("127.0.0.1", 9000)
    asyncio.run(run())


def test_connection_refused_and_server_restart():
    async def run():
        server = BootstrapServer("127.0.0.1", 0, 9000)
        await server.start()
        addr = server.local_addr
        await server.stop()
        await server.stop()  # Idempotent.
        with pytest.raises(BootstrapError, match="connection_failed"):
            await bootstrap_connect(addr)
        with pytest.raises(RuntimeError):
            _ = server.local_addr
        try:
            await server.start()
            assert await bootstrap_connect(server.local_addr) == ("127.0.0.1", 9000)
        finally:
            await server.stop()
    asyncio.run(run())


def test_bootstrap_then_udp_login_and_chat(tmp_path):
    """Test harness wires A2 to the real app; production integration is B1."""
    async def run():
        users = Path(__file__).resolve().parents[1] / "server/data/users.json"
        server = CollabServer("127.0.0.1", 0, str(users))
        bootstrap = None
        clients = []
        try:
            await server.endpoint.start(("127.0.0.1", 0))
            port = server.endpoint.local_addr[1]
            # Let the OS allocate a TCP port independently. A randomly selected
            # UDP port is not guaranteed to be available for a TCP listener.
            bootstrap = BootstrapServer("127.0.0.1", 0, port)
            await bootstrap.start()
            for username in ("alice", "bob"):
                udp_addr = await bootstrap_connect(bootstrap.local_addr)
                assert udp_addr == ("127.0.0.1", port)
                client = CollabClient(udp_addr, str(tmp_path / username))
                clients.append(client)
                await client.start()
                assert (await client.login(username, username + "123"))[0]
            alice, bob = clients
            alice.send_chat("after bootstrap", to="bob")

            async def receive_chat():
                while True:
                    event = await bob.events.get()
                    if event["event"] == "chat":
                        return event

            event = await asyncio.wait_for(receive_chat(), 2)
            assert event["text"] == "after bootstrap"
            assert event["from"] == "alice" and event["private"]
        finally:
            for client in clients:
                await client.stop()
            if bootstrap:
                await bootstrap.stop()
            await server.endpoint.stop()
    asyncio.run(run())
