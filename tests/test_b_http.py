import asyncio
import json
from pathlib import Path
from client.client import CollabClient
from client.web_ui.http_server import WebUI, MAX_BODY
from tests.app_helpers import session, wait_until


async def request(
    ui, path="/", body=b"", method="GET", extra=b"", split=False, length=None
):
    reader, writer = await asyncio.open_connection("127.0.0.1", ui.port)
    try:
        writer.write(
            f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{ui.port}\r\nContent-Length: {len(body) if length is None else length}\r\n".encode()
            + extra
            + b"\r\n"
        )
        if split:
            for i in range(0, len(body), 1000):
                writer.write(body[i : i + 1000])
                await writer.drain()
                await asyncio.sleep(0)
        else:
            writer.write(body)
        await writer.drain()
        response = await asyncio.wait_for(reader.read(), 10)
        status = int(response.split(b" ", 2)[1])
        _, _, payload = response.partition(b"\r\n\r\n")
        return status, payload
    finally:
        writer.close()
        await writer.wait_closed()


def test_offline_ui_and_bad_http(tmp_path):
    async def run():
        c = CollabClient(("127.0.0.1", 1))
        ui = WebUI(c, 0)
        await ui.start()
        try:
            assert (await request(ui))[0] == 200
            state = json.loads((await request(ui, "/api/state"))[1])
            assert not state["connected"] and state["transport_stats"] is None
            assert (await request(ui, "/api/login", b"[]", "POST"))[0] == 400
            assert (await request(ui, "/api/login", b"{}", "POST", length=-1))[0] == 400
            assert (
                await request(
                    ui,
                    "/api/login",
                    b"{}",
                    "POST",
                    extra=b"Origin: https://untrusted.example\r\n",
                )
            )[0] == 403
            assert (await request(ui, "/api/login", b"", "POST", length=65537))[
                0
            ] == 413
            assert (await request(ui, "/static/../../server/auth.py"))[0] == 404
        finally:
            await ui.stop()
        assert not Path(ui.outbox).exists()

    asyncio.run(run())


def test_stream_upload_unique_names_cleanup_and_limits(tmp_path):
    async def run():
        async with session(tmp_path) as (_, (a, b)):
            ui = WebUI(a, 0)
            await ui.start()
            try:
                payload = b"content" * 2000
                result = []
                for data in (payload, payload + b"2"):
                    status, body = await request(
                        ui, "/api/file?to=bob&name=same.txt", data, "POST", split=True
                    )
                    assert status == 200
                    result.append(json.loads(body)["file_id"])
                await wait_until(
                    lambda: all(fid in b._incoming_offers for fid in result)
                )
                assert len(list(Path(ui.outbox).rglob("same.txt"))) == 2
                paths = [a._outgoing[fid][0].path for fid in result]
                assert Path(paths[0]).read_bytes() == payload
                assert Path(paths[1]).read_bytes() == payload + b"2"
                for fid in result:
                    b.reject_file(fid)
                await wait_until(lambda: not a._outgoing)
                assert not list(Path(ui.outbox).rglob("same.txt"))
                assert (
                    await request(
                        ui,
                        "/api/file?to=bob&name=f",
                        method="POST",
                        length=MAX_BODY + 1,
                    )
                )[0] == 413
                assert (
                    await request(ui, "/api/file?to=bob&name=..%2Funsafe", b"x", "POST")
                )[0] == 400
            finally:
                await ui.stop()

    asyncio.run(run())


def test_truncated_upload_removes_partial(tmp_path):
    async def run():
        async with session(tmp_path) as (_, (a, b)):
            ui = WebUI(a, 0)
            await ui.start()
            try:
                r, w = await asyncio.open_connection("127.0.0.1", ui.port)
                w.write(
                    f"POST /api/file?to=bob&name=f HTTP/1.1\r\nHost: 127.0.0.1:{ui.port}\r\nContent-Length: 1000\r\n\r\npartial".encode()
                )
                await w.drain()
                w.write_eof()
                await asyncio.wait_for(r.read(), 3)
                w.close()
                await w.wait_closed()
                assert not list(Path(ui.outbox).iterdir())
                assert not a._outgoing
            finally:
                await ui.stop()

    asyncio.run(run())


def test_sse_one_tab_and_cleanup():
    async def run():
        c = CollabClient(("127.0.0.1", 1))
        ui = WebUI(c, 0)
        await ui.start()
        r, w = await asyncio.open_connection("127.0.0.1", ui.port)
        try:
            w.write(
                f"GET /api/events HTTP/1.1\r\nHost: 127.0.0.1:{ui.port}\r\n\r\n".encode()
            )
            await w.drain()
            assert b"200 OK" in await r.readuntil(b"\r\n\r\n")
            assert (await request(ui, "/api/events"))[0] == 409
        finally:
            w.close()
            await w.wait_closed()
            await ui.stop()
        assert not ui._handlers and not ui._sse_active

    asyncio.run(run())
