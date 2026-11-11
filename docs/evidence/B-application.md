# B application test evidence

Date: 10 November 2026. Base revision: `4b67076`. Code tested: B implementation working tree on `feature/b-application-completion`. Platform: Linux x86_64, Python 3.12.14. No GitHub push was performed.

## Baseline and final result

Unmodified base code was exported with git archive and run separately: **22 passed, 4 skipped**. After B changes, a separate copy of source excluding Git/cache/downloads was run using the same installed Python runtime:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q
```

**51 passed, 2 skipped in 21.08 seconds**. Raw final stdout and runtime details are in `pytest-B-final.txt`. Disabling third-party pytest plugin auto-loading avoids an environment-only anyio plugin deprecation warning; the tests use core pytest fixtures and asyncio.run. Earlier normal invocations also passed, with that external deprecation warning.

This is a clean source copy, not a newly provisioned OS or independently installed virtualenv. Windows/macOS and two physical LAN machines have not been tested. Static checks: Ruff formatting on B-owned changed Python files, F-category undefined/unused checks passed; git diff --check passed. These are not claims of whole-repository PEP8 or complete security compliance.

## Implemented scenarios

- Existing packet/reliability/basic app tests remain passing.
- Salted credential uniqueness and rejection of old bare SHA-256 storage.
- Wrong/duplicate/repeated login; no user identity change on the same connection; one registry connection cannot own multiple names.
- Private chat is received by the designated recipient and not the third online user.
- Files of 0, 1, 8192 and 8193 bytes with a Unicode filename pass real localhost relay and receiver hash, with sender FILE_DONE.
- Invalid sizes (bool/negative/string/above limit), overflow, spoofed participants, duplicate ACCEPT and premature DONE rejected.
- File rejection, disconnect while offer pending, abrupt client crash, disconnect during a partially written file and cancellation of its producer clean application state.
- Reconnect gets a new connection and authenticates again; logout clears credentials and retry task.
- Enqueue failure still cleans the transfer when FILE_ERROR itself cannot be enqueued.
- Bootstrap ordering/lifecycle/failure and future ACK receipt behavior tested against API stubs. **Real A2/A5 are absent**, so those tests do not certify actual TCP or bounded transport integration.
- HTTP tests exercise an offline daemon, malformed JSON, negative/oversized Content-Length, cross-origin and path rejection, streaming body fragments, same-name uploads, partial upload deletion and single active SSE connection/cleanup.

## Lossy relay test

`tests/test_file_transfer.py::test_file_over_lossy_network` uses 32,768 bytes (`bytes(range(256)) * 128`). After login, each endpoint's outbound path is impaired:

| Endpoint | Loss | Delay | Jitter | Duplicate | Seed |
|---|---:|---:|---:|---:|---:|
| Server | 0.10 | 0.001 s | uniform 0..0.003 s | 0.02 | 31 |
| Sender | 0.10 | 0.001 s | uniform 0..0.003 s | 0.02 | 32 |
| Receiver | 0.10 | 0.001 s | uniform 0..0.003 s | 0.02 | 33 |

This covers sender->server->receiver DATA and reverse ACK/signaling. Assertions check receiver byte-for-byte equality (and the application's SHA-256/DONE result), sender successful DONE, nonzero simulated drops and removal of relay/application transfer state. Handshake/login is not impaired in this test. This one deterministic scenario does not prove correctness for every loss pattern or file size.

## Twenty clients

`tests/test_b_application.py::test_load_twenty_clients` starts a real localhost UDP server, then runs the same load engine as `python -m tests.load_test`. A login barrier precedes broadcasts. Each of twenty clients expects exactly nineteen authenticated messages, each from a different peer, and checks a bounded late-duplicate observation period.

Observed separate sample: **380 expected, 380 received, zero duplicates**; 0.228 s login phase, 0.612 s total measurement and 1,293,111 peak traced Python bytes in the generator process. The sample is recorded in `B-load-example.json`. Timing is machine-specific, the duplicate observation is finite and Python traced memory is not server RSS. No large-file memory bound is claimed.

## Remaining skips and limitations

The original A placeholders `test_no_head_of_line_blocking` and `test_peer_disconnect_detected` remain skipped. Both B skips (lossy file and app crash cleanup) now contain assertions and are active.

Real A2 TCP bootstrap, A5 async relay/consume/bounded queues, A6 control security/endpoint cleanup, CUBIC validation, 10/100 MB memory tests, slow receiver, long server outage/restart and two-machine LAN remain open. Existing v1 pending/reassembly buffers are unbounded. Application cleanup does not reset v1 transport streams.

A browser smoke run was attempted but Chromium did not launch: the packaged Playwright browser was absent and the system Chromium process failed with `socket() failed: Operation not permitted` before opening a page. No browser UI/XSS runtime pass or screenshots are claimed. HTTP tests passed; client-side textContent usage was reviewed statically. PDF report pages were rendered and visually inspected separately.
