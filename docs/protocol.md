# Đặc tả giao thức  [A+B – Bước 1]

> Cập nhật A6, 11/11/2026: handshake kiểm input trước thay phiên, tối đa 64 connection/Endpoint mặc định, lifecycle/socket cleanup có kiểm thử. **CUBIC mặc định**, wire giữ **v2** với credit, async API và authenticated control của A5. Bootstrap A2 chờ B1 tích hợp lifecycle thật; handshake vẫn chưa xác thực danh tính. [A4](A4-cubic.md), [A5](A5-flow-control.md), [A6](A6-security-resilience.md).
> Audit và bằng chứng: [AUDIT_A.md](AUDIT_A.md), [baseline](evidence/A1-baseline.md). Mọi thay đổi API/wire phải cập nhật tài liệu và test cùng lần tích hợp.
> Kiểm thử A7: [HoL, disconnect, receiver chậm và loss](A7-transport-tests.md); wire/API giữ nguyên. Hai stub transport đã có assertion và cleanup; test app/file B6 và LAN J3 là phạm vi riêng.

## 1. Tầng transport (UDP tin cậy) – do A phụ trách

### 1.1 Header gói tin (23 byte, big-endian, `struct "!BBBHIIIIH"`)

| Trường | Kích thước | Ý nghĩa |
|---|---|---|
| version | 1 B | Phiên bản giao thức (hiện = 2; v1 bị từ chối) |
| ptype | 1 B | Loại gói (bảng 1.2) |
| flags | 1 B | bit0 = EOM (gói cuối của một message) |
| stream_id | 2 B | Số hiệu stream (mỗi chiều đánh số độc lập) |
| conn_id | 4 B | Do server cấp khi handshake |
| seq | 4 B | Số thứ tự gói **trong stream**, tăng dần từ 0 (chỉ DATA) |
| ack | 4 B | ACK tích lũy: mọi seq < ack đã nhận (chỉ ACK) |
| sack | 4 B | Bitmap: bit i = đã nhận seq `ack+1+i` (chỉ ACK) |
| length | 2 B | Độ dài wire payload, gồm tag và counter nếu là control |

### 1.2 Loại gói
`DATA=0, ACK=1, HELLO=2, HELLO_REPLY=3, PING=4, PONG=5, CLOSE=6, CREDIT=7, CREDIT_PROBE=8, RESET_STREAM=9, RESET_ACK=10, STOP_STREAM=11`

### 1.3 Streams
- `0` = control (AUTH, danh sách user, tín hiệu file)
- `1` = chat (MSG_BROADCAST, MSG_PRIVATE)
- `>=2` = mỗi file một stream riêng. Reassembly và async consumer độc lập; cwnd vẫn dùng chung. A5/A7 kiểm handler file chậm không chặn chat. A7 còn giữ DATA seq 0 của stream file (gồm cả retransmit) cho đến khi chat được giao, rồi mới thả packet và kiểm file hoàn tất, trên cả CUBIC và AIMD.
- Mỗi stream có không gian `seq` và cửa sổ riêng; congestion control dùng chung cho cả kết nối.

### 1.3b Message-oriented
`send(data)` được cắt thành các gói ≤ `MAX_PAYLOAD`; gói cuối có cờ EOM. Bên nhận ghép lại và giao **nguyên một message**.

### 1.4 UDP handshake hiện có (chưa đáp ứng TCP bootstrap Step 2)
```
Client                                   Server
  | HELLO(payload = X25519 pub key) -->   |
  |   <-- HELLO_REPLY(conn_id, server pub)|
  | (cả hai: HKDF -> khóa c2s, s2c)       |
  | DATA(stream 0, AUTH) -------------->  |   (đã mã hóa AES-GCM)
```

### 1.5 Mã hóa (wire v2)

HELLO/HELLO_REPLY trao đổi public key X25519 32 byte. HKDF-SHA256: salt=None, info=`collab-app v2`, output 64 byte chia thành khóa c2s và s2c 32 byte. DATA dùng AES-256-GCM/tag 16 byte.

DATA nonce: `00 || uint16_be(sid) || uint32_be(seq) || 5 zero bytes`. AAD là header với length gồm tag. Tạo bản mã đúng một lần, cache nguyên wire bytes để retransmit; bỏ cache khi ACK/cancel/close. Control nonce/counter riêng ở mục 6. Server đọc plaintext để relay, không phải E2E. Handshake chưa xác thực danh tính, chưa chống MITM chủ động.

### 1.6 Trạng thái

Code chưa có enum state: Endpoint dùng `_pending`/`connections`; Connection dùng `closed`. Client chờ HELLO_REPLY tối đa 4 lượt, mỗi lượt 1,5 giây. Server tạo Connection khi nhận HELLO hợp lệ; client tạo khi nhận HELLO_REPLY. Chỉ phía server gọi `on_connect`; client lấy conn_id từ kết quả `await connect()`.

`close()` gửi CLOSE best effort rồi đóng ngay, không có pha CLOSING chờ peer ACK. Timeout/retry exhaustion gọi `_finish()`. Endpoint xóa connection và HELLO cache rồi gọi disconnect callback. [Sơ đồ hiện tại và dự kiến](diagrams/transport_a1.md).

### 1.7 Ví dụ header v2

`Packet(PKT_PING, conn_id=7).header_bytes(24)` trước khi nối counter/tag cho PING body rỗng:

```text
02 04 00 0000 00000007 00000000 00000000 00000000 0018
ver type flags sid conn_id seq ack sack length
```

Header hex: `0204000000000000070000000000000000000000000018` (23 byte). Wire PING dài 47 byte: header + counter 8 byte + tag 16 byte. Helper encode(Packet) chỉ framing; Connection mới tạo control envelope.

## 2. Tầng ứng dụng (JSON trong payload) – do B phụ trách

| type | Hướng | Trường | Stream |
|---|---|---|---|
| AUTH | C→S | username, password | 0 |
| AUTH_OK / AUTH_FAIL | S→C | users[] / reason | 0 |
| USER_LIST, USER_JOINED, USER_LEFT | S→C | users[] / username | 0 |
| MSG_BROADCAST | C→S→C | text (+from) | 1 |
| MSG_PRIVATE | C→S→C | to, text (+from) | 1 |
| FILE_OFFER | C→S→C | to, file_id, name, size, sha256 | 0 |
| FILE_ACCEPT / FILE_REJECT | C→S→C | to, file_id | 0 |
| FILE_BEGIN | C→S→C | to, file_id, size (message đầu của stream file) | ≥2 |
| (chunk) | C→S→C | bytes thô, mỗi message = 1 chunk | ≥2 |
| FILE_DONE | C→S→C | to, file_id, ok | 0 |
| FILE_ERROR / ERROR | S→C | reason | 0 |

### Luồng truyền file
1. A gửi `FILE_OFFER` → server chuyển cho B (thêm `from`).
2. B gửi `FILE_ACCEPT` → server chuyển cho A.
3. A mở stream mới, gửi `FILE_BEGIN` rồi các chunk. Server mở stream mới tới B và chuyển tiếp.
4. B nhận đủ `size` byte → kiểm SHA-256 → gửi `FILE_DONE{ok}`.

## 3. Điều khiển luồng / tắc nghẽn / gửi lại

### 3.1 Hành vi hiện tại

- Datagram mục tiêu 1200 byte; DATA plaintext tối đa `1200 - 23 - 16 = 1161` byte. Một message rỗng vẫn tạo một DATA EOM. File chunk hiện là 8192 byte/message.
- Mỗi chiều, mỗi stream có sequence riêng từ 0. Reorder nhận `[expected, expected+64)`; ACK là `expected`; bit SACK i xác nhận `expected+1+i`, chỉ phủ 32 vị trí. Các packet xa hơn vẫn được giữ nếu trong cửa sổ 64 nhưng chưa SACK được. Duplicate không giao lại; vẫn phát ACK.
- Sender A3 chỉ phát mới khi inflight<cwnd và seq<cumulative_ack+64. SACK giải phóng inflight không cho phép vượt cửa sổ sequence. ACK/SACK được kiểm toàn bộ trước mutation: ack không vượt highest_sent+1; mọi SACK phải trỏ packet đã phát. ACK sai bị bỏ và tăng invalid_acks. ACK cũ hợp lệ có thể xác nhận thêm SACK nhưng không làm cumulative_ack lùi.
- cwnd/ssthresh/inflight tính bằng packet trên toàn connection; cwnd khởi đầu 10, trần 64. Mặc định CUBIC theo RFC 9438 với C=0,4, beta=0,7; Reno slow start, tăng theo thời gian/RTT và ACK mới, Reno-friendly và fast convergence. ACK packet thuộc flight đã lấp đầy floor(cwnd) được phép tăng cửa sổ khi flight thoát dần; flight thưa do thiếu dữ liệu/credit không làm tăng. Khoảng application/credit-limited không có flight đã dùng đủ cửa sổ được loại khỏi thời gian CUBIC. Fast loss đặt ssthresh=cwnd=max(0,7*min(cwnd_cũ,flight_trước_ACK),2); timeout dùng cùng ngưỡng và cwnd=1, giai đoạn avoidance đầu sau timeout có K=0. Giới hạn min với cwnd cũ là lựa chọn bảo thủ để timeout escalation không tăng lại cửa sổ/ngưỡng. [Công thức và phạm vi A4](A4-cubic.md).
- `Endpoint(congestion_algorithm="aimd")` giữ baseline cũ: slow start +1/packet mới ACK, avoidance +1/cwnd; fast loss giảm một nửa, timeout về 1. Chọn thuật toán cục bộ cho từng Endpoint, không negotiation qua wire. Stats/log lấy algorithm thật từ controller; mỗi connection có trạng thái riêng.
- RTT chỉ đo packet có retries=0. Mẫu đầu: SRTT=R, RTTVAR=R/2. Sau đó RTTVAR=0,75*RTTVAR+0,25*abs(SRTT_cũ-R); SRTT=0,875*SRTT+0,125*R. RTO=clamp(SRTT+4*RTTVAR, 0,2s, 5s), ban đầu 0,5s. Đây là cấu hình dự án, không tuyên bố tuân thủ toàn bộ RFC.
- Fast retransmit A3: mỗi packet thiếu đếm tối đa 3 sequence khác nhau lớn hơn nó vừa được ACK. Mỗi sequence chỉ rời unacked một lần nên ACK lặp không thêm bằng chứng. Đủ 3 thì queue packet thiếu; fast retransmit tối đa một lần/packet. Nếu bản gửi lại tiếp tục mất thì phục hồi bằng RTO.
- Khi phát mới, packet snapshot RTO nền làm retry_interval và deadline=sent_at+retry_interval. Tick 0,02s kiểm now>=deadline. Khi thực sự retransmit do timeout: retry_interval=min(2*retry_interval,5s), deadline=now+retry_interval. Fast retransmit chỉ đặt lại deadline, không nhân interval. Không gọi RttEstimator.backoff(): RTT sample mới không đổi deadline các packet đang bay, không nhân backoff theo cả packet lẫn connection.
- Retransmit queue FIFO theo (stream_id,seq), không có entry trùng; timeout có thể nâng lý do fast đang chờ thành timeout. Một packet retransmit tối đa mỗi TICK_INTERVAL=0,02s trên connection, chung cho cả fast/timeout. ACK có thể kích hoạt slot ngay nếu đã đến hạn; không bù nhiều slot khi tick trễ. Gửi mới vẫn tuân cwnd; retransmit được phép tiến triển khi inflight>=cwnd sau giảm cửa sổ. Đây là ngân sách recovery của dự án, chưa phải pacing toàn bộ DATA hoặc bảo đảm công bằng (A5).
- Recovery epoch snapshot highest_sent riêng mỗi stream có unacked, không so seq giữa stream. Chỉ giảm vì fast loss một lần khi vào epoch; có thể chuyển sang timeout và giảm theo timeout một lần trong epoch đó. ACK trong lúc recovery không tăng cwnd; kết thúc khi không còn unacked thuộc snapshot, không chờ DATA mới gửi sau snapshot. ACK loại cả entry retransmit đang chờ.
- MAX_RETRIES=10 tính chung fast+timeout/packet; lần hết deadline sau 10 lần gửi lại đóng với reason `too many retries`. Peer timeout có thể xảy ra trước. Close/timeout dọn pending, unacked, wire cache, receive buffers và retransmit queue; callback disconnect một lần. DATA chỉ dùng seq 0..2^32-2; enqueue vượt giới hạn đóng với reason `sequence exhausted`, không wrap.
- PING sau 2s không gửi; peer timeout sau 10s không nhận packet hợp lệ. Control tag/replay và DATA decrypt được kiểm trước cập nhật liveness. A6 đã thêm validation các field không dùng, handshake và cap connection; kiểm thử và giới hạn bảo mật ở [A6](A6-security-resilience.md).
- A5 đã thêm credit byte/packet-slot, queue có trần, consume và scheduler như mục 5–7. ACK chứng minh transport giữ dữ liệu, chưa chứng minh app xử lý xong. Không bảo đảm exactly-once qua reconnect.

### 3.2 Đơn vị thống nhất

| Đại lượng | Đơn vị |
|---|---|
| cwnd, ssthresh, inflight, retry budget, sequence window | packet DATA |
| pending, unacked payload, receive buffers, credit, ACK progress | byte plaintext payload; không gồm header/tag |
| wire cache | byte datagram, đo riêng để kiểm RAM |
| RTT/RTO/elapsed | giây, monotonic; không dùng wall clock |
| seq/ack | chỉ số packet trong một chiều của stream |

## 4. API sync tương thích trên v2

```python
Endpoint(is_server=False, *, flow_limits=None)
set_handlers(on_connect=None, on_receive=None, on_disconnect=None)
await start(local_addr=("0.0.0.0", 0))
await connect(remote_addr) -> int
open_stream(conn_id) -> int
send(conn_id, stream_id, data: bytes) -> bool
close(conn_id, reason="closed locally")
await stop()
```

Callbacks sync: on_connect(conn_id,addr), on_receive(conn_id,sid,data), on_disconnect(conn_id,reason). Không truyền coroutine vào callback sync. send=True chỉ enqueue; False khi đầy/đóng/không có connection. Message quá lớn raise MessageTooLarge, stream đã hủy raise StreamCancelled. local_addr chỉ đọc khi đang chạy. Chỉ một handshake pending/Endpoint.

Stop đóng connection, đánh thức send/ACK/reset/handshake waiter, cancel rồi await tick/consumer tasks và đóng socket. Start/stop được serialize; start lặp báo RuntimeError, stop lặp idempotent, start lại sau stop được phép. Stop chờ callback đóng socket: close bình thường tối đa 0,05s, nếu còn I/O thì abort rồi chờ callback tối đa 0,5s; hết hạn vẫn raise TimeoutError. Các reason A3 giữ nguyên (`closed locally`, `closed by peer`, `timeout`, `too many retries`, `sequence exhausted`); có endpoint_stopped, protocol_error, receive handler error, control counter exhausted. A6 thêm peer_restarted (HELLO mới hợp lệ cùng địa chỉ), socket_closed (socket mất), transport_error (lỗi tick ngoài dự kiến). B ánh xạ reason sang UI theo hợp đồng này.

`Endpoint(max_connections=64)` giới hạn tổng transport session, gồm cả chưa auth; cho phép cấu hình số nguyên 1..65535. Khi đầy, bỏ HELLO tạo phiên mới, không loại peer đang hoạt động. HELLO trùng của phiên có sẵn vẫn nhận reply cache. HELLO/reply phải có key đúng 32 byte, flags/stream/seq/ack/sack=0; HELLO.conn_id=0, reply.conn_id!=0. Kiểm key hợp lệ và cấp ID mới trước khi thay phiên cũ. Reply phải từ đúng địa chỉ pending, không đè conn_id đã dùng. HELLO không có key confirmation hoặc identity authentication. Auth-specific cap/deadline còn thuộc B.

Endpoint có counters tổng `stats`: invalid_packets, invalid_handshakes, resource_rejections, socket_errors, processing_errors; giữ qua restart. Không dùng các counters này thay TransportStats từng connection. Lỗi socket/process tại Endpoint được ghi log tối đa một lần/giây/Endpoint. `error_received`/sendto OSError không đóng toàn bộ peer; socket thật sự connection_lost thì fail các phiên và pending handshake, dọn cache và dừng timer. Khi quá trình tick của một connection có lỗi ngoài dự kiến, cô lập connection đó bằng transport_error.

## 5. Hợp đồng Endpoint đã triển khai A5

### 5.1 Gửi, ACK và ownership

```python
await ep.send_message(conn_id, stream_id, data: bytes) -> SendReceipt
await receipt.wait_acked() -> None
receipt.message_id: int
receipt.payload_bytes: int
receipt.acked_bytes: int
ep.get_stats(conn_id) -> TransportStats
ep.get_peer_addr(conn_id) -> tuple[str, int]
```

- Chấp nhận nguyên một message vào queue giới hạn. Return khi enqueue thành công, không chờ ACK; không enqueue một nửa message. FIFO theo thứ tự chấp nhận trong stream. Một producer/stream là cách dùng khuyến nghị.
- Kiểm kích thước trước khi chờ. Chờ khi ngân sách pending+unacked hoặc số message đầy; receipt ACK tăng khi từng packet được ACK mới, tối đa payload_bytes. Duplicate ACK/retransmit không tăng tiến độ.
- `bytes` bất biến do transport giữ tham chiếu hoặc copy có tính vào ngân sách. Trong lúc chờ, caller giữ data; B phải giới hạn số producer/task đang gọi, không tạo task cho toàn bộ file.
- Cancellation trước enqueue không gửi byte nào. Sau khi return, hủy task chờ ACK không thu hồi DATA; muốn bỏ stream gọi cancel_stream. Không có await giữa commit enqueue và return để tránh trạng thái commit không rõ.
- FILE_BEGIN và từng chunk có receipt riêng. B chỉ cộng receipt của chunk cho tiến độ file, không cộng JSON BEGIN/header/tag. ACK chặng client→server chưa phải người nhận cuối DONE.
- Hàm `send()` cũ được giữ như try-enqueue có trần: False khi đầy/đóng, không silently drop sau khi trả True. B phải chuyển các nơi cần delivery sang API async và xử lý lỗi; không tiếp tục bỏ qua False.

### 5.2 Nhận và trả credit

Chọn consumer async **theo stream**, thay vì tự gọi callback async mà không await:

```python
ep.set_message_handler(handler) -> None
async def handler(message: ReceivedMessage) -> None: ...
# message.conn_id, message.stream_id, message.data, message.delivery_id
ep.consume(message) -> None
```

- Mode mới loại trừ `on_receive` cũ, chọn trước start. Dispatcher queue được chặn bởi credit byte và packet-slot của Connection. Một handler đang chạy/stream; các stream khác được chạy độc lập. Control vẫn FIFO trong stream 0, chat FIFO trong stream 1.
- Receipt nhận chỉ tạo sau reassembly EOM; transport giữ credit cho fragments, reorder, queue và message đang được xử lý. ACK chỉ sau khi dữ liệu được giữ trong ngân sách đã cấp, không ACK rồi bỏ vì queue đầy.
- B gọi consume sau ghi/xử lý xong, hoặc sau `await send_message()` sang chặng relay kế tiếp đã nhận ownership trong queue có trần. Không cần chờ ACK chặng sau mới consume. Xóa tham chiếu app sau consume; giữ bản copy riêng phải nằm trong ngân sách app.
- Consume idempotent theo delivery_id, không tăng credit hai lần; token thuộc connection khác bị từ chối. Token đã dọn do cancel/close trở thành no-op. Handler return mà chưa consume giữ credit; transport không tự cấp thêm. Handler lỗi reset file stream hoặc đóng connection nếu là control/chat.
- Mode callback sync cũ chỉ dùng khi callback xử lý xong dữ liệu trước return; không dùng cho relay async. B1/B3 chuyển sang mode mới để hưởng backpressure toàn tuyến.

### 5.3 Đóng, hủy và lỗi

```python
await ep.cancel_stream(conn_id, stream_id, reason="cancelled") -> None
ep.close(conn_id, reason="closed locally") -> None
await ep.stop() -> None
```

Stream identity là `(conn_id, sender_role, stream_id)` vì mỗi chiều có sequence riêng. cancel_stream nhắm chiều **gửi cục bộ**, chỉ dành sid>=2. Chấm dứt stream đó, bỏ pending/unacked/cache, đánh thức mọi waiter với StreamCancelled. Receiver yêu cầu dừng truyền qua message FILE_ERROR của B; không vô tình hủy stream cùng số ở chiều ngược lại.

Reset gửi tin cậy theo mục 6; giữ tombstone để DATA tới muộn không tái tạo stream. Không tái sử dụng stream ID dưới cùng khóa. Close idempotent; mọi send/ACK waiter nhận ConnectionClosed, consumer/task/future được dọn. Stop cancel rồi await background tasks, đóng socket và handshake đang chờ; ứng dụng B dọn file/task của mình.

Lỗi API: MessageTooLarge(ValueError), StreamLimitError(RuntimeError), StreamCancelled(ConnectionError), ConnectionClosed(ConnectionError). QueueFull(BufferError) dùng bên trong try-enqueue; Endpoint.send trả False, send_message chờ thay vì trả lỗi này. TransportProtocolError là type dự trữ; hiện protocol error đóng connection và waiter nhận ConnectionClosed. Chuỗi reason thực tế ở mục 4, chưa thay tất cả reason cũ bằng mã thiết kế A1. cancel_stream(reason=...) dùng reason cho lỗi local; wire reset gửi mã 1.

TransportStats là snapshot, không lộ mutable Connection: state, reason, algorithm, srtt_seconds (None nếu chưa đo), rto_seconds, cwnd_packets, inflight_packets, pending_bytes, unacked_bytes, wire_cache_bytes, recv_buffered_bytes, acked_payload_bytes, retransmitted_packets, timeout_count; có breakdown per-stream. ACK bytes là payload transport; B ánh xạ receipt để loại metadata file.

## 6. Wire v2 đã triển khai A5

### 6.1 Version và nonce

Giữ header 23 byte và sequence per-stream, PROTOCOL_VERSION=2. V1 bị từ chối, không fallback. Bootstrap mặc định trả version=2. HELLO/HELLO_REPLY vẫn mang public key 32 byte; HKDF info `collab-app v2` phân tách khóa với v1.

- DATA nonce v2: `00 || uint16_be(sid) || uint32_be(seq) || 5 zero bytes`.
- Control nonce v2: `01 || uint64_be(counter) || 3 zero bytes`.
- Khóa riêng mỗi chiều; counter tăng từ 0 trên toàn chiều gửi connection, không riêng từng type. Không wrap. Mỗi DATA tạo bản mã đúng một lần với AAD/header bất biến, cache nguyên datagram để retransmit.
- Control đã thiết lập phiên gồm ACK/PING/PONG/CLOSE và type mới CREDIT=7, CREDIT_PROBE=8, RESET_STREAM=9, RESET_ACK=10, STOP_STREAM=11. Payload wire: `counter_u64_be || AESGCM(body)`; AAD=`header || counter`; length bao gồm 8-byte counter, body và tag 16 byte. Counter plaintext nhưng được xác thực. Payload DATA tối đa vẫn 1161; control body tối đa 1153 byte.
- Mỗi lần gửi control, kể cả nhắc lại credit/reset, dùng counter mới. Receiver giữ replay bitmap 256 counter; xác thực tag trước cập nhật replay/liveness. Counter cũ ngoài cửa sổ hoặc đã thấy bị loại; control hợp lệ bị reorder trong cửa sổ được chấp nhận. Không ghi nhận counter từ gói tag sai.
- DATA duplicate hợp lệ không giao app lại nhưng ACK bằng counter control mới. Nhờ vậy mất ACK vẫn phục hồi. HELLO chưa xác thực không được dùng cập nhật liveness của connection đang thiết lập.

### 6.2 Body control và credit

Tất cả số nguyên big-endian; body phải đúng độ dài. ACK dùng sid/ack/sack của header, body rỗng. PING/PONG body rỗng; CLOSE body reason_code u8 (0 normal, 1 timeout, 2 retry, 3 protocol, 4 resource, 5 stop). Các field header không dùng phải bằng 0; flags control=0.

CREDIT dùng header.sid chỉ chiều gửi của peer. Body **6 uint64 big-endian (48 byte)**: stream_byte_limit, conn_byte_limit, file_conn_byte_limit, stream_packet_limit, conn_packet_limit, file_conn_packet_limit. Đây là giới hạn tích lũy, không phải cộng thêm. Sender lấy max của từng limit hợp lệ, không cộng lại khi update cũ/trùng. Mỗi DATA mới tốn payload bytes và một packet-slot; retransmit không tốn thêm. Packet-slot bổ sung so với thiết kế A1 để message rỗng không làm metadata vô hạn. Giữ counters kể cả stream đã hủy.

CREDIT_PROBE dùng sid, body rỗng; peer đáp bằng CREDIT mới nhất. Khi chờ credit gửi probe sau RTO, backoff tới 5s, một probe outstanding/stream; limit update cũ/trùng không tăng credit. Receiver chỉ tăng limit tương ứng byte đã consume; giới hạn slot/message cũng phải giữ chỗ (mục 7).

RESET_STREAM/RESET_ACK: sid ở header, body `final_bytes_u64 || final_packets_u64 || reason_u8` (17 byte), với tổng DATA duy nhất **đã phát**, không tính pending. reset nhắm chiều gửi cục bộ, sid>=2. Sender bỏ pending/unacked, fail receipt/waiter bằng StreamCancelled, retry reset theo RTO/backoff/MAX_RETRIES với counter mới. ACK phải echo đúng body. Receiver kiểm final không nhỏ hơn đã nhận, không vượt credit đã cấp, rồi ghi nhận phần thiếu và trả credit đúng một lần; token cũ thành no-op, giữ tombstone, DATA tới muộn không tạo lại stream. Hết reset retry đóng connection. ID không reuse.

STOP_STREAM=11: body reason_u8; receiver có handler file lỗi yêu cầu peer cancel chiều gửi tương ứng. Gửi lại yêu cầu tới khi nhận RESET_STREAM hoặc hết retry. Không hủy stream cùng số ở chiều ngược lại. Control/chat handler lỗi đóng connection. B vẫn phải dọn file state/FILE_ERROR theo B3/B4.

### 6.3 Validation và recovery

- Decode kiểm version, type, độ dài và MAX_DATAGRAM; Connection kiểm conn_id/version, tag/replay/control flags/seq/body size, stream cap và ACK range. DATA chỉ dùng FLAG_EOM, ack/sack=0, seq!=2^32-1, payload wire tối thiểu một GCM tag. PING/PONG/CLOSE dùng sid=0; control không dùng seq. CLOSE reason trong 0..5. Các field header/độ dài control kiểm trước tag/replay; body ngữ nghĩa kiểm sau tag, nên counter của control có tag hợp lệ nhưng body sai có thể vẫn được ghi nhận. Tag sai không thay replay/liveness/credit. ACK sai phạm vi không giải phóng buffer hoặc tăng cwnd. [Test A6](A6-security-resilience.md).
- DATA seq tối đa `2^32-2`, để ACK kế tiếp `2^32-1` còn biểu diễn được. Dừng/tạo phiên mới trước khi dùng hết seq/stream/counter; không wrap và không reuse nonce. Stream ID 2..65535, thêm trần tài nguyên mục 7.
- Sender giữ cửa sổ sequence 64 tính từ cumulative ACK, ngoài cwnd và credit. Bitmap SACK 32 giữ nguyên; packet xa hơn có thể cần chờ ACK tích lũy hoặc timeout.
- A3 đã dùng 3 sequence phía sau được ACK/SACK mới làm bằng chứng fast retransmit; cùng một ACK lặp không tăng bằng chứng. RTO xử lý tail loss và loss retransmit; không lấy RTT từ retransmit. V2 giữ quy tắc này sau bước xác thực control.
- Recovery boundary đã có ở A3 như mục 3.1: snapshot highest-sent theo từng stream, một fast-loss reduction và tối đa một timeout escalation trong epoch. V2 thêm kết thúc phần snapshot khi stream reset hợp lệ. Retransmit có ngân sách riêng để tiến triển dù inflight>=cwnd. Backoff theo packet có trần, không nhân RTO theo số packet trong batch.
- A4 đã triển khai/bật CUBIC với clock monotonic inject được và giữ AIMD làm baseline. Công thức/tham số, lựa chọn packet accounting, test thời gian giả, trace và so sánh ở [A4-cubic.md](A4-cubic.md). Việc chọn controller không thay header, control body, nonce hay wire version.

## 7. Giới hạn và scheduling A5

Giá trị đã áp dụng trong FlowLimits, không phải số đo RAM hay yêu cầu rubric. MiB=1048576 byte, KiB=1024 byte. Thay đổi phải cập nhật spec/test, không nới riêng một phía. V2 dùng cùng profile cố định, chưa thương lượng profile.

| Tài nguyên | Trần áp dụng |
|---|---|
| Connection/Endpoint | A6: mặc định 64 transport session, cấu hình max_connections; deadline/giới hạn theo auth thuộc B |
| Stream mỗi chiều/connection | 32 active (gồm control/chat), 1024 ID từng dùng kể cả tombstone; hết thì phiên mới |
| Message control/chat | 64 KiB |
| Message file (gồm BEGIN/chunk) | 16 KiB; chunk B hiện 8192 byte |
| Pending + unacked payload | 256 KiB/stream, 2 MiB/connection |
| Receive credit ban đầu | 256 KiB/stream, 2 MiB/connection |
| Message pending+unacked | 1024/stream, 4096/connection |
| DATA packet-slot nhận chưa consume | 1024/stream, 4096/connection, reserve 128 slot cho control/chat |
| Reorder | 64 packet/stream; nằm trong receive byte budget |
| Wire cache | chỉ packet unacked; accounting riêng, không giữ lịch sử ACK |
| ACK/control không phải DATA | không bị data credit chặn; coalesce ACK/credit theo stream, không queue vô hạn |

Message cap không vượt credit byte/packet-slot ban đầu để tránh chờ EOM trong khi sender hết credit. Mỗi DATA, kể cả payload rỗng, chiếm một receive packet-slot tới khi message được consume. Không ACK dữ liệu nếu không giữ được trong budget. Fragments vượt message cap/thiếu EOM tới mức vượt cap đóng với protocol_error. Reorder vẫn tối đa 64 packet/stream.

Dành 128 KiB connection capacity/credit cho DATA stream 0/1: file chỉ sử dụng 2 MiB-128 KiB chưa consume tại một thời điểm. `conn_limit = 2 MiB + total_consumed`; `file_conn_limit = (2 MiB-128 KiB) + file_consumed`; `stream_limit = 256 KiB + stream_consumed`. Sender kiểm cumulative unique bytes mới của stream, connection và file class trước khi phát. Theo dõi riêng control/chat/file usage khi cấp limit để stream file không chiếm reserve. Phần cấp trước chưa dùng cũng là nghĩa vụ giữ chỗ; tổng grant connection phải được bảo đảm, không chỉ nhìn bytes đã tới. Mỗi chiều theo dõi ledger của từng stream và tổng connection; byte/slot trên từng connection được chặn; A6 thêm cap connection toàn Endpoint theo bảng trên.

Scheduler dùng vòng weighted round-robin có con trỏ giữ qua các pump: tối đa 4 DATA control/chat rồi một lượt DATA file nếu file đủ điều kiện. File round-robin; không có chat thì file dùng phần rỗi, không có file thì chat dùng phần rỗi. ACK/credit/control bảo trì coalesce theo (type,sid), tối đa 16 frame mỗi tick 0,02s, không cần DATA credit. Retransmit theo ngân sách A3 riêng. Đo latency và khả năng file tiến triển bằng test, không suy từ tên scheduler.

## 8. TCP bootstrap A2 — đã có module, chờ B1 tích hợp

API tại `transport/bootstrap.py`: `BootstrapServer(host, tcp_port, udp_port, version=PROTOCOL_VERSION, *, timeout=3.0)` với `await start()`/`await stop()` và `local_addr`; `await bootstrap_connect(server_addr, *, version=PROTOCOL_VERSION, timeout=3.0) -> (peer_ip, udp_port)`. B gọi bootstrap trước UDP connect ở startup/reconnect, stop listener khi dừng server. Hiện PROTOCOL_VERSION=2.

Framing JSON UTF-8 một dòng, tối đa 4096 byte gồm newline, không giả định một read là một message. Request hiện tại `{"type":"BOOTSTRAP","version":2}`; response `{"type":"BOOTSTRAP_ACK","version":2,"udp_port":9000}`. Version đã đổi đồng bộ với UDP. Validate type, integer version (không nhận bool), udp_port 1..65535; timeout connect/read, EOF sớm/JSON lỗi/oversize đều thất bại và đóng writer. TCP ACK ở đây là message ứng dụng, khác ACK của hệ điều hành.

Client raise `BootstrapError(ConnectionError)` với thuộc tính `.reason`: timeout, connection_failed, unexpected_eof, frame_too_large, invalid_frame, invalid_response, version_mismatch. Sai cấu hình local (port/version/timeout) raise ValueError. Timeout 3 giây tính cho toàn bộ connect + request drain + đọc frame; cleanup writer có timeout riêng tối đa 0,5 giây. Task cancellation vẫn propagate CancelledError và đóng writer. Không fallback UDP khi bootstrap thất bại.

Server dùng timeout cho cả nhận request và drain ACK. Request không hợp lệ/sai version/oversize/EOF/timeout bị đóng, không nhận ACK thành công; client có thể thấy unexpected_eof thay vì reason chi tiết của server. Stop idempotent, đóng listener và mọi accepted writer, cancel rồi await handler tasks. Có thể start lại sau stop; start hai lần khi đang chạy raise RuntimeError. `local_addr` chỉ đọc khi chạy; tcp_port=0 hỗ trợ test, udp_port phải là cổng UDP đã bind thật (1..65535).

Host UDP là **IP peer TCP đã kết nối**, không resolve hostname lại hoặc tin một host tùy ý từ response. B dùng cùng số cổng TCP/UDP 9000 trong cấu hình mặc định; constructor nhận tường minh cả hai. Sau ACK đóng TCP; chat/file tiếp tục UDP. Module bootstrap không tự auth. B phụ trách reconnect và thông báo UI.

Test module: `tests/test_bootstrap.py`; có test harness nối TCP → UDP → login → DM bằng app hiện tại, chưa thay đổi lifecycle production. [Bàn giao và kết quả A2](A2-bootstrap.md). B1 phải nối vào ứng dụng rồi kiểm đường chạy thật trước khi nghiệm thu toàn bộ Step 2.

B cần review các điểm: receipt riêng BEGIN/chunk; chuyển receive sang async handler+consume; relay queue có trần; reset theo chiều gửi; lỗi/stats; auth timeout và bootstrap lifecycle. Đây là hợp đồng bàn giao của A, chưa ghi nhận B đã tích hợp hoặc đồng ý. Thay đổi sau review phải cập nhật hợp đồng và test liên quan.
