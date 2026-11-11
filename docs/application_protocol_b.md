# Application protocol implemented by B

This supplements docs/protocol.md without changing A's transport wire format. App messages are JSON objects encoded as UTF-8. Binary chunks are separate messages, never decoded as JSON. Message sender identity is taken from the authenticated server registry, not a client-provided `from` field.

| Stream | Messages | Rules |
|---|---|---|
| 0 | AUTH, LOGOUT, user updates, FILE_OFFER/ACCEPT/REJECT/DONE/ERROR | Authentication/state and file signaling |
| 1 | MSG_PRIVATE, MSG_BROADCAST | Auth required; 1-4096 text characters; private `to` is required |
| >=2 | FILE_BEGIN then raw chunks | New stream per transfer, sender and server outbound stream IDs are independent |

FILE_OFFER: `to`, `file_id` (1-64 ASCII letters/digits/underscore/hyphen), `name` (1-255 chars, no slash/backslash/NUL, not dot or dot-dot), integer `size` in [0, 200 MiB] excluding bool, lowercase 64-character SHA-256.

FILE_ACCEPT and FILE_REJECT: receiver -> original sender, same file_id, state must be offered. Repeated answers and third-party answers are rejected. A file_id is unique among active server offers.

FILE_BEGIN: original sender -> original recipient, file_id and size must match the accepted offer. Server opens its own outbound stream and sends BEGIN with `from` instead of `to`. Receiver verifies the matching accepted offer, sender and size. A file cannot bind multiple active receive streams.

Each raw chunk is at most 8192 bytes and may not exceed remaining declared bytes. Size 0 immediately enters await_done at server and completes hash verification at receiver. Receiver creates destination files exclusively (never overwrites an existing file), verifies exact byte count/SHA-256 and deletes invalid or aborted output.

FILE_DONE: receiver -> original sender, boolean `ok`; accepted only after all declared bytes have been relayed. Server cannot independently verify the receiver's disk hash. A malicious authorized receiver can lie about success; this is not a signed receipt.

FILE_ERROR: either participant may abort only its own known transfer; server informs both participants and removes application relay state. An unaffiliated sender cannot cancel someone else's file. Server also aborts offers/active transfers on disconnect or 120 seconds without file activity. These do not reset/cancel transport queues in v1; that is pending A5.

Server states: offered -> accepted -> relaying -> await_done -> removed. Zero-byte files skip relaying. Rejection, peer error, disconnect and inactivity lead to removal. No resume across connections. Pending offers are included in the initial SSE snapshot; history and interrupted transfers are not persisted.

Progress events carry `stage`: queued (v1), acked_to_server (A5 receipt adapter) or received. A completed progress bar is not a successful transfer until FILE_DONE. See docs/AUDIT_B.md for capabilities, limits and pending integration.
