# B application audit and delivery checklist

Base: `4b67076` on main. Implementation branch: `feature/b-application-completion`.
Review date: 10 November 2026. This is an application handoff, not confirmation that all ten rubric steps pass.

## Authoritative sources and example code

- Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application.docx, supplied by the student: sections 6-10 and the ten-row rubric. Step 2 explicitly asks for a TCP connection and acknowledgement; Step 5 uses server routing; Step 6 explicitly uses UDP; Step 9 asks for HTTP UI with client-side JavaScript. The text also mentions TCP file/message transfer in Techniques. The project's TCP-bootstrap/UDP-data design is an interpretation of these inconsistencies, not an instructor clarification.
- CO3094_assignment_streaprous.pdf: pp. 7-8 describe raw TCP; pp. 8-11 describe HTTP authentication and P2P chat; p. 12 states that Assignment 1 requirements and grading follow the established Assignment 1 description. P2P/channels, raw TCP implementation and HTTP cookies are not silently added to the ten-step rubric. Record the discrepancy if the instructor expects these supplemental tasks as well.
- [Current work allocation](https://github.com/SPKILLER2301/Distributed-Streaming-based-Client-Server/blob/main/WORKPLAN.md): B1-B7 and J1-J4. WORKPLAN takes precedence over stale ownership comments in README/report_outline.
- [Instructor example repository](https://github.com/SPKILLER2301/temp2), path `CO3094_streaprous/`: `daemon/backend.py` contains `async_server` using asyncio.start_server, a standard socket implementation and a raw TCP implementation. The presence of the async branch supports this implementation choice but is not a grading approval.

The example is an unfinished teaching scaffold: `Request.prepare` does not initialize parsed headers/body before using them; HttpAdapter has unimplemented route hooks and raw response sending; its coroutine uses `addr` before assignment. `apps/sampleapp.py` login only returns a welcome response. These are read from the code; no claim is made that its demo was successfully run. Example source was not copied into this deliverable. Its LICENSE specifies restricted educational use; preserve the example's notices and follow course distribution instructions.

The supplied database assignment DOCX was confirmed to be an accidental upload and is excluded.

## Task checklist

| Task | Work implemented in this branch | Remaining acceptance work |
|---|---|---|
| B1 | Message field limits/types; file state checks; server/client bootstrap lifecycle adapter; TCP-before-UDP ordering test and failure-without-fallback test | A2's `transport/bootstrap.py` is absent. Tests use the documented API stub. Run real TCP ACK, timeout/version/framing tests after A2 arrives; default currently logs explicit UDP-only baseline |
| B2 | Salted PBKDF2 demo accounts; hashing outside receive callback; one login task per connection; repeated login same identity; duplicate username/connection guard; IP/port from connect callback; auth deadline and attempt cap; DM isolation | Review with A's endpoint capacity/close contract; demo credentials remain public and are not production accounts |
| B3 | OFFER/ACCEPT/BEGIN/DONE participants/state; file and offer caps; Unicode; zero-byte completion; size/hash checks; exclusive destination creation; FILE_ERROR relay, disconnect/timeout cleanup; queued progress distinct from receiver DONE | A5 receive credit, bounded relay/send queues, stream reset and per-chunk receipt integration are not available. Sender has a documented receipt adapter tested with a stub; relay still uses v1 sync callbacks. 10/100 MB bounded-memory/slow-receiver acceptance remains blocked |
| B4 | One reconnect supervisor; backoff+jitter; re-auth with memory-only credentials; logout/stop cancel retries; cancel producers/delete partials; no implicit file resume; UI starts before server connection | Test real A2 on every reconnect, A6 pending-handshake cancellation, restart/long outage and two-machine LAN |
| B5 | 64 KiB upload reads; 200 MiB file cap; 2 simultaneous uploads; timeouts; unique per-upload directories; upload temp cleanup; 64 KiB JSON cap; bad framing/origin/Host/path rejection; one SSE tab per daemon; heartbeat/cleanup; separate HTTP and UDP state | Real RTT/retransmit requires A's get_stats. Current UI explicitly displays unavailable stats. Student has manually verified browser login/chat/file accept-reject/reconnect on Windows; large-file memory test and final stats remain; do not claim full B5 acceptance |
| B6 | Both B skips replaced with real tests; 10% loss/dup/jitter across both relay hops; end-to-end file hash; crash cleanup; 20-client login barrier and exact 380 message assertions; failing load checks exit nonzero | Large-file RAM bounds and HoL latency need A5/A7. Student-reported Windows/Python 3.13.3 run: 51 passed, 2 skipped in 22.13 s after fixing the chat test race. No two-machine LAN, macOS run or server RSS result yet |
| B7 | Updated README, app/file protocol supplement, chat/file Mermaid diagrams, reproducible test evidence, technical report draft PDF and source packaging | A8 final protocol/security/CUBIC content, LAN results, instructor ambiguity resolution and final clean-machine review required before submission |

Do not mark a row complete merely because code exists. Final integration depends on A2/A5/A6 and shared J2-J4.

## Application behavior and limits

Passwords are stored as `pbkdf2_sha256$260000$salt_hex$digest_hex`; the bundled demo passwords remain `username + 123`. Old bare SHA-256 credential files are rejected; recreate credentials explicitly rather than treating bare hashes as PBKDF2.

At most 16 active offers/transfers per participant; 200 MiB per file; file inactivity deadline 120 seconds at the server; chat text 1-4096 characters; auth deadline 10 seconds with up to five credential checks per connection. These are application limits, not proof of a transport memory bound. UI history is capped at 2048 events and browser chat history at 500 rows; offline history is not durable.

The UI is local-only, one active SSE tab per daemon. Two users should launch two daemon processes on different UI ports. Uploads are binary HTTP bodies with Content-Length; chunked transfer encoding is rejected. JSON, header and request-line budgets are separate from the file size budget.

File progress `queued` is local enqueue only. `acked_to_server` is emitted only when the future A5 receipt resolves; it still does not mean receiver success. Only receiver FILE_DONE after size/SHA-256 verification is end-to-end success. Transfers are not resumed after reconnect.

Transport v1's pending/reassembly queues, unauthenticated control packets and unauthenticated key exchange remain A-owned limitations. Do not use this demo with confidential data or publish real user credentials/files in logs. Use synthetic accounts and test files for demonstrations.

## Handoff to A

1. Supply `BootstrapServer(host, tcp_port, udp_port, version)` and `bootstrap_connect(server_addr, version=..., timeout=...)` from `transport.bootstrap`. B's start/stop/connect hooks are already present. Bootstrap errors propagate; an installed but broken module does not silently fall back.
2. Supply Endpoint `send_message`, SendReceipt.wait_acked, async receive handler + consume, cancel_stream, get_stats and stable close errors as in docs/protocol.md. Convert Router's receive path and forwarding to async+consume together; enabling only the sender receipt does not bound the relay.
3. Run A7 HoL/timeout tests and validate memory limits, retransmit/security/CUBIC. B has not changed transport files or constants.
4. Test `TCP ACK -> UDP -> AUTH -> chat/file -> receiver DONE`, simulated loss on both hops, sender/receiver crash and server restart; finish LAN on two physical machines and clean install.
