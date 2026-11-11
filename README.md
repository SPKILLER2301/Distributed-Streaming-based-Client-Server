# Distributed streaming collaboration application

Python chat and file-sharing demo: a local HTTP/JavaScript UI talks to a client daemon, which communicates with the central server over the project's encrypted reliable-UDP transport. Application work is owned by B; transport/crypto/CUBIC/bootstrap by A.

**Integration status:** this branch improves application correctness, reconnect, uploads and testing. Current main has no `transport/bootstrap.py`, receive credit/backpressure, CUBIC or public transport stats API. The application logs **UDP baseline only** until A2 arrives. It does not claim all ten rubric steps are complete. See [B audit and checklist](docs/AUDIT_B.md).

## Install

Use Python 3.10 or later. Tested here on Python 3.12/Linux. The student also reports Python 3.13.3/Windows: 51 passed, 2 skipped, plus manual browser chat/file/reconnect checks. macOS and two-machine LAN remain untested.

```bash
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Run a server and two clients

Terminal 1:

```bash
python -m server.main --host 127.0.0.1 --port 9000
```

Terminal 2:

```bash
python -m client.main --server 127.0.0.1:9000 --ui-port 8080
```

Terminal 3:

```bash
python -m client.main --server 127.0.0.1:9000 --ui-port 8081
```

Open http://127.0.0.1:8080 and log in as `alice` / `alice123`. Open http://127.0.0.1:8081 and log in as `bob` / `bob123`. Use **one UI tab per daemon**; a second simultaneous SSE connection receives 409. Other demo accounts are carol, dave and user_001 through user_020; passwords are always username + `123`. Credentials are salted PBKDF2 records in server/data/users.json; no real accounts belong in this demo file.

1. Send a global chat message. Click an online username to select private chat; click it again to return to global chat.
2. Select a recipient, choose a file and wait for the recipient to click **Nhận** or **Từ chối**.
3. Files are saved in the recipient daemon's `downloads/` folder relative to its working directory, with unique suffixes if a filename exists.
4. A queued/ACKed progress count is not final delivery. Wait for **hoàn tất, hash hợp lệ**, based on receiver FILE_DONE. Zero-byte files also complete.
5. On disconnect, partial files are removed and active file state is aborted. Reconnect uses backoff/jitter and memory-only login credentials; there is no automatic file resume. Logout and stop erase credentials and cancel retries.

The HTTP UI starts even if the collaboration server is offline. A login request while offline returns a clear error and schedules connection attempts. HTTP/SSE connectivity and UDP-server connectivity are displayed separately. RTT/retransmission values are explicitly unavailable until A supplies get_stats.

## Networking and A handoff

For a LAN demonstration, bind server to `--host 0.0.0.0` and use its actual LAN IPv4 address in each client's `--server` argument. The UI remains bound to 127.0.0.1. Configure the host firewall for the server UDP port; when A2 is integrated, allow the TCP bootstrap port too. This is a procedure to test, not a claim of completed LAN testing.

If `transport.bootstrap` is installed, B automatically calls `bootstrap_connect` before UDP connect, including reconnect, and manages BootstrapServer start/stop. Bootstrap failures do not fall back to UDP. Server `--tcp-port` defaults to the UDP port. Until then, --server is the UDP baseline address. B's adapter tests use stubs; real A2 integration remains open.

Current v1 send/relay queues are not bounded by the UI's streaming upload limits. Avoid large-file acceptance claims until A5 supplies async receive+consume, bounded send queues and cancellation; the sender already has an adapter for SendReceipt.wait_acked. Full relay conversion remains required. The v1 key exchange does not authenticate server identity and control packets are not authenticated; do not use confidential data in demonstrations.

## Verify

```bash
python -m pytest -q
```

Full evidence and limitations: [test evidence](docs/evidence/B-application.md). Both B placeholder skips have been implemented. The remaining two transport skips belong to A. Tests include real localhost UDP transfers, empty/boundary/Unicode files, 10% simulated loss across both relay hops, crash cleanup, reconnect, HTTP framing/upload/SSE and 20 concurrent clients.

With the server already running:

```bash
python -m tests.load_test --server 127.0.0.1:9000 --clients 20
```

The load runner waits for all logins before sending broadcasts, expects exactly 380 deliveries with zero duplicates, and exits nonzero on failures/timeouts. Its reported memory is the load process's traced Python allocations, not server RSS or a bound on large-file memory.

## Documents and responsibilities

- [WORKPLAN](WORKPLAN.md): official A/B project allocation; stale TODO owner comments are superseded by this table.
- [Hướng dẫn người B](docs/HUONG_DAN_NGUOI_B.md): checklist tiếng Việt, chạy thử và áp dụng bản vá.
- [B audit](docs/AUDIT_B.md): B1-B7 status, source comparison and handoff requirements.
- [Application protocol](docs/application_protocol_b.md) and [chat/file sequences](docs/diagrams/application_b.md).
- [Transport protocol](docs/protocol.md): A's current baseline and planned interface, not a claim that v2 exists.
- [Technical report draft](docs/report.pdf): actual implementation, tests and explicit incomplete integration. Final PDF still needs A8 and LAN results.

The course example at https://github.com/SPKILLER2301/temp2 was reviewed for reference, not copied or executed with raw-socket/firewall changes. It is an unfinished scaffold with its own educational license. The submitted Assignment 1 DOCX is the rubric source; supplementary PDF raw-TCP/P2P requirements differ and should be confirmed with the instructor before claiming those requirements are met.
