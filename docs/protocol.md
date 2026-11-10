# Đặc tả giao thức  [A+B – Bước 1]

> A1 thiết kế; cập nhật A3 ngày 10/11/2026: mục 1–4 mô tả UDP v1 hiện tại, đã có reliability A3. Mục 5–7 là hợp đồng A4–A6; các phần chưa triển khai vẫn được ghi rõ. Module bootstrap A2 ở mục 8 đã có/test, chờ B1 nối startup/reconnect thật. Chưa có xác nhận review hợp đồng từ B.
> Audit và bằng chứng: [AUDIT_A.md](AUDIT_A.md), [baseline](evidence/A1-baseline.md). Mọi thay đổi API/wire phải cập nhật tài liệu và test cùng lần tích hợp.

## 1. Tầng transport (UDP tin cậy) – do A phụ trách

### 1.1 Header gói tin (23 byte, big-endian, `struct "!BBBHIIIIH"`)

| Trường | Kích thước | Ý nghĩa |
|---|---|---|
| version | 1 B | Phiên bản giao thức (hiện = 1) |
| ptype | 1 B | Loại gói (bảng 1.2) |
| flags | 1 B | bit0 = EOM (gói cuối của một message) |
| stream_id | 2 B | Số hiệu stream (mỗi chiều đánh số độc lập) |
| conn_id | 4 B | Do server cấp khi handshake |
| seq | 4 B | Số thứ tự gói **trong stream**, tăng dần từ 0 (chỉ DATA) |
| ack | 4 B | ACK tích lũy: mọi seq < ack đã nhận (chỉ ACK) |
| sack | 4 B | Bitmap: bit i = đã nhận seq `ack+1+i` (chỉ ACK) |
| length | 2 B | Độ dài payload (đã mã hóa nếu là DATA) |

### 1.2 Loại gói
`DATA=0, ACK=1, HELLO=2, HELLO_REPLY=3, PING=4, PONG=5, CLOSE=6`

### 1.3 Streams
- `0` = control (AUTH, danh sách user, tín hiệu file)
- `1` = chat (MSG_BROADCAST, MSG_PRIVATE)
- `>=2` = mỗi file một stream riêng. Reassembly độc lập nên không có phụ thuộc thứ tự giữa file và chat; cwnd/scheduler vẫn dùng chung, chưa có bằng chứng test HoL vì test còn skip.
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

### 1.5 Mã hóa (Bước 8)
X25519 trao đổi public key 32 byte trong HELLO/HELLO_REPLY. HKDF-SHA256: salt=None, info=`collab-app v1`, output 64 byte, chia thành khóa c2s và s2c, mỗi khóa 32 byte. DATA dùng AES-256-GCM, tag 16 byte.

Nonce chính xác: `uint16_be(stream_id) || uint32_be(seq) || 00 00 00 00 00 00` (12 byte). AAD là toàn bộ header với length bao gồm tag. Từ A3, DATA mã hóa đúng một lần lúc phát mới, giữ nguyên datagram trong InFlight.wire_data và retransmit bytes đó. Cache được bỏ cùng packet khi ACK hoặc connection đóng. Không được đổi plaintext **hoặc AAD** khi tái dùng cặp khóa/nonce. ACK/PING/PONG/CLOSE hiện không xác thực. Handshake chưa xác thực danh tính, chưa chống MITM chủ động. Mã hóa từng chặng client–server; server đọc plaintext để relay, không phải E2E.

### 1.6 Trạng thái

Code chưa có enum state: Endpoint dùng `_pending`/`connections`; Connection dùng `closed`. Client chờ HELLO_REPLY tối đa 4 lượt, mỗi lượt 1,5 giây. Server tạo Connection khi nhận HELLO hợp lệ; client tạo khi nhận HELLO_REPLY. Chỉ phía server gọi `on_connect`; client lấy conn_id từ kết quả `await connect()`.

`close()` gửi CLOSE best effort rồi đóng ngay, không có pha CLOSING chờ peer ACK. Timeout/retry exhaustion gọi `_finish()`. Endpoint xóa connection và HELLO cache rồi gọi disconnect callback. [Sơ đồ hiện tại và dự kiến](diagrams/transport_a1.md).

### 1.7 Ví dụ encoder v1

`encode(Packet(PKT_PING, conn_id=7))` (không mã hóa):

```text
01 04 00 0000 00000007 00000000 00000000 00000000 0000
ver type flags sid conn_id seq ack sack length
```

Chuỗi liền: `0104000000000000070000000000000000000000000000` (23 byte).
Đây là PING v1; control v2 bên dưới có counter/tag nên không dùng cùng ví dụ bytes.

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
- cwnd/ssthresh/inflight tính bằng packet trên toàn connection; cwnd khởi đầu 10, trần 64. AIMD tăng +1 mỗi ACK mới trong slow start, sau đó +1/cwnd. Fast loss đặt ssthresh=cwnd=max(cwnd_cũ/2,2); timeout đặt ssthresh=max(cwnd_cũ/2,2), cwnd=1. CUBIC chưa có (A4).
- RTT chỉ đo packet có retries=0. Mẫu đầu: SRTT=R, RTTVAR=R/2. Sau đó RTTVAR=0,75*RTTVAR+0,25*abs(SRTT_cũ-R); SRTT=0,875*SRTT+0,125*R. RTO=clamp(SRTT+4*RTTVAR, 0,2s, 5s), ban đầu 0,5s. Đây là cấu hình dự án, không tuyên bố tuân thủ toàn bộ RFC.
- Fast retransmit A3: mỗi packet thiếu đếm tối đa 3 sequence khác nhau lớn hơn nó vừa được ACK. Mỗi sequence chỉ rời unacked một lần nên ACK lặp không thêm bằng chứng. Đủ 3 thì queue packet thiếu; fast retransmit tối đa một lần/packet. Nếu bản gửi lại tiếp tục mất thì phục hồi bằng RTO.
- Khi phát mới, packet snapshot RTO nền làm retry_interval và deadline=sent_at+retry_interval. Tick 0,02s kiểm now>=deadline. Khi thực sự retransmit do timeout: retry_interval=min(2*retry_interval,5s), deadline=now+retry_interval. Fast retransmit chỉ đặt lại deadline, không nhân interval. Không gọi RttEstimator.backoff(): RTT sample mới không đổi deadline các packet đang bay, không nhân backoff theo cả packet lẫn connection.
- Retransmit queue FIFO theo (stream_id,seq), không có entry trùng; timeout có thể nâng lý do fast đang chờ thành timeout. Một packet retransmit tối đa mỗi TICK_INTERVAL=0,02s trên connection, chung cho cả fast/timeout. ACK có thể kích hoạt slot ngay nếu đã đến hạn; không bù nhiều slot khi tick trễ. Gửi mới vẫn tuân cwnd; retransmit được phép tiến triển khi inflight>=cwnd sau giảm cửa sổ. Đây là ngân sách recovery của dự án, chưa phải pacing toàn bộ DATA hoặc bảo đảm công bằng (A5).
- Recovery epoch snapshot highest_sent riêng mỗi stream có unacked, không so seq giữa stream. Chỉ giảm vì fast loss một lần khi vào epoch; có thể chuyển sang timeout và giảm theo timeout một lần trong epoch đó. ACK trong lúc recovery không tăng cwnd; kết thúc khi không còn unacked thuộc snapshot, không chờ DATA mới gửi sau snapshot. ACK loại cả entry retransmit đang chờ.
- MAX_RETRIES=10 tính chung fast+timeout/packet; lần hết deadline sau 10 lần gửi lại đóng với reason `too many retries`. Peer timeout có thể xảy ra trước. Close/timeout dọn pending, unacked, wire cache, receive buffers và retransmit queue; callback disconnect một lần. DATA chỉ dùng seq 0..2^32-2; enqueue vượt giới hạn đóng với reason `sequence exhausted`, không wrap.
- PING sau 2s không gửi; peer timeout sau 10s không nhận. Hiện last_recv cập nhật cả trước xác thực DATA và với type chưa biết: cần sửa A6.
- Không có receive credit/backpressure; ACK transport không chứng minh app đã xử lý. Callback lỗi sau ACK có thể làm app mất message. Không có bảo đảm exactly-once qua reconnect.

### 3.2 Đơn vị thống nhất

| Đại lượng | Đơn vị |
|---|---|
| cwnd, ssthresh, inflight, retry budget, sequence window | packet DATA |
| pending, unacked payload, receive buffers, credit, ACK progress | byte plaintext payload; không gồm header/tag |
| wire cache | byte datagram, đo riêng để kiểm RAM |
| RTT/RTO/elapsed | giây, monotonic; không dùng wall clock |
| seq/ack | chỉ số packet trong một chiều của stream |

## 4. API Endpoint hiện có (v1)

```python
Endpoint(is_server: bool = False)
set_handlers(on_connect=None, on_receive=None, on_disconnect=None) -> None
await start(local_addr=("0.0.0.0", 0)) -> None
await connect(remote_addr) -> int  # chỉ UDP, một handshake pending/Endpoint
open_stream(conn_id: int) -> int
send(conn_id: int, stream_id: int, data: bytes) -> bool
close(conn_id: int) -> None
await stop() -> None
```

Callbacks đều **sync**: `on_connect(conn_id, addr)`, `on_receive(conn_id, stream_id, data)`, `on_disconnect(conn_id, reason)`. Không truyền coroutine vào callback này. `send=True` nghĩa đã enqueue, chưa ACK; False khi connection không tồn tại/đã đóng. `open_stream` với conn_id không tồn tại hiện có thể raise KeyError. `local_addr` chỉ đọc sau start.

`stop()` hiện cancel tick nhưng không await kết thúc task, chưa đánh thức/cancel handshake future. Các reason hiện có là chuỗi `closed locally`, `closed by peer`, `timeout`, `too many retries`, `sequence exhausted`. Stats nội bộ A3 gồm sent, retransmitted, sent_payload_bytes, acked_packets, acked_payload_bytes, fast_retransmitted, timeout_retransmitted, timeout_count, invalid_acks, recovery_epochs, loss_reductions, timeout_reductions. timeout_count đếm các tick có packet timeout mới đưa vào queue, không phải số lần packet phát lại. ACK byte chỉ đếm plaintext mới một lần. Stats công khai/receipt per-message vẫn thuộc A5. App hiện còn đọc `endpoint.connections[conn_id].remote_addr`; API v2 phải có accessor thay thế.

## 5. Hợp đồng Endpoint dự kiến (A5/A6; chưa có trong code)

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

- Mode mới loại trừ `on_receive` cũ, chọn trước start. A5 cung cấp dispatcher với queue byte/message có trần. Một handler đang chạy/stream; các stream khác được chạy độc lập. Control vẫn FIFO trong stream 0, chat FIFO trong stream 1.
- Receipt nhận chỉ tạo sau reassembly EOM; transport giữ credit cho fragments, reorder, queue và message đang được xử lý. ACK chỉ sau khi dữ liệu được giữ trong ngân sách đã cấp, không ACK rồi bỏ vì queue đầy.
- B gọi consume sau ghi/xử lý xong, hoặc sau `await send_message()` sang chặng relay kế tiếp đã nhận ownership trong queue có trần. Không cần chờ ACK chặng sau mới consume. Xóa tham chiếu app sau consume; giữ bản copy riêng phải nằm trong ngân sách app.
- Consume idempotent theo delivery_id, không tăng credit hai lần; token thuộc connection khác bị từ chối. Token đã dọn do cancel/close trở thành no-op. Handler return mà chưa consume giữ credit; transport không tự cấp thêm. Handler lỗi reset file stream hoặc đóng connection nếu là control/chat.
- Mode callback sync cũ chỉ dùng khi callback xử lý xong dữ liệu trước return; không dùng cho relay async. B1/B3 chuyển sang mode mới để hưởng backpressure toàn tuyến.

### 5.3 Đóng, hủy và lỗi

```python
await ep.cancel_stream(conn_id, stream_id, reason="cancelled") -> None
ep.close(conn_id, reason="closed_local") -> None
await ep.stop() -> None
```

Stream identity là `(conn_id, sender_role, stream_id)` vì mỗi chiều có sequence riêng. cancel_stream nhắm chiều **gửi cục bộ**, chỉ dành sid>=2. Chấm dứt stream đó, bỏ pending/unacked/cache, đánh thức mọi waiter với StreamCancelled. Receiver yêu cầu dừng truyền qua message FILE_ERROR của B; không vô tình hủy stream cùng số ở chiều ngược lại.

Reset gửi tin cậy theo mục 6; giữ tombstone để DATA tới muộn không tái tạo stream. Không tái sử dụng stream ID dưới cùng khóa. Close idempotent; mọi send/ACK waiter nhận ConnectionClosed, consumer/task/future được dọn. Stop cancel rồi await background tasks, đóng socket và handshake đang chờ; ứng dụng B dọn file/task của mình.

Lỗi API mới: `MessageTooLarge(ValueError)`, `StreamLimitError(RuntimeError)`, `StreamCancelled(ConnectionError)`, `ConnectionClosed(ConnectionError)`, `TransportProtocolError(ConnectionError)`. Reason ổn định: closed_local, closed_peer, peer_timeout, retry_exhausted, protocol_error, resource_limit, endpoint_stopped. Không đưa raw exception text thành mã UI.

TransportStats là snapshot, không lộ mutable Connection: state, reason, algorithm, srtt_seconds (None nếu chưa đo), rto_seconds, cwnd_packets, inflight_packets, pending_bytes, unacked_bytes, wire_cache_bytes, recv_buffered_bytes, acked_payload_bytes, retransmitted_packets, timeout_count; có breakdown per-stream. ACK bytes là payload transport; B ánh xạ receipt để loại metadata file.

## 6. Wire/security dự kiến v2 (quyết định A1, triển khai A5/A6)

### 6.1 Version và nonce

Giữ header 23 byte và sequence per-stream; tăng PROTOCOL_VERSION lên **2 khi triển khai**. V1/v2 không fallback im lặng. Bootstrap trả version tương ứng. HELLO/HELLO_REPLY vẫn trao đổi public key 32 byte; HKDF info mới `collab-app v2` phân tách khóa với v1, vẫn chia c2s/s2c. A1 không sửa constants đang chạy.

- DATA nonce v2: `00 || uint16_be(sid) || uint32_be(seq) || 5 zero bytes`.
- Control nonce v2: `01 || uint64_be(counter) || 3 zero bytes`.
- Khóa riêng mỗi chiều; counter tăng từ 0 trên toàn chiều gửi connection, không riêng từng type. Không wrap. Mỗi DATA tạo bản mã đúng một lần với AAD/header bất biến, cache nguyên datagram để retransmit.
- Control đã thiết lập phiên gồm ACK/PING/PONG/CLOSE và type mới CREDIT=7, CREDIT_PROBE=8, RESET_STREAM=9, RESET_ACK=10. Payload wire: `counter_u64_be || AESGCM(body)`; AAD=`header || counter`; length bao gồm 8-byte counter, body và tag 16 byte. Counter plaintext nhưng được xác thực. Payload DATA tối đa vẫn 1161; control body tối đa 1153 byte.
- Mỗi lần gửi control, kể cả nhắc lại credit/reset, dùng counter mới. Receiver giữ replay bitmap 256 counter; xác thực tag trước cập nhật replay/liveness. Counter cũ ngoài cửa sổ hoặc đã thấy bị loại; control hợp lệ bị reorder trong cửa sổ được chấp nhận. Không ghi nhận counter từ gói tag sai.
- DATA duplicate hợp lệ không giao app lại nhưng ACK bằng counter control mới. Nhờ vậy mất ACK vẫn phục hồi. HELLO chưa xác thực không được dùng cập nhật liveness của connection đang thiết lập.

### 6.2 Body control và credit

Tất cả số nguyên big-endian; body phải đúng độ dài. ACK dùng sid/ack/sack của header, body rỗng. PING/PONG body rỗng; CLOSE body reason_code u8 (0 normal, 1 timeout, 2 retry, 3 protocol, 4 resource, 5 stop). Các field header không dùng phải bằng 0; flags control=0.

CREDIT dùng header.sid chỉ stream chiều ngược lại mà peer đang gửi; body `stream_limit_u64 || conn_limit_u64 || file_conn_limit_u64` (24 byte). Đây là tổng byte **được phép gửi kể từ đầu phiên**, không phải số byte cộng thêm. Receiver cấp ban đầu theo mục 7 và gửi cập nhật sau consume; sender lấy max của limit hợp lệ, không cộng lại. Retransmit không chiếm credit mới. Giữ tổng sent/consumed theo chiều connection, kể cả stream đã đóng; không làm giảm các bộ đếm tích lũy khi dọn stream. File phải thỏa thêm file_conn_limit; nhờ vậy reserve control/chat có thể kiểm tra từ wire, không phụ thuộc state bí mật của peer.

CREDIT_PROBE dùng sid, body rỗng; peer đáp bằng CREDIT mới nhất. Khi chờ credit gửi probe sau RTO, backoff tới 5s, một probe outstanding/stream; limit update cũ/trùng không tăng credit. Receiver chỉ tăng limit tương ứng byte đã consume; giới hạn slot/message cũng phải giữ chỗ (mục 7).

RESET_STREAM/RESET_ACK: sid ở header, body `final_bytes_u64 || reason_u8`, với final_bytes là tổng DATA plaintext duy nhất đã thực sự phát trên stream đó (không tính pending chưa gửi). Người gửi reset retransmit semantic request với counter mới, theo RTO/MAX_RETRIES; peer xác nhận cùng final_bytes. Receiver kiểm final_bytes không nhỏ hơn dữ liệu đã nhận, không vượt stream limit đã cấp và phần ngân sách connection còn hợp lệ; ghi nhận bytes bị bỏ như đã consume đúng một lần để trả connection credit, không cấp thêm cho stream đã reset. RESET_ACK lặp không thay counters. Hết retry đóng connection để tránh trạng thái treo không rõ. Tombstone giữ cuối cùng và xử lý reset trùng; không khôi phục DATA đến muộn.

### 6.3 Validation và recovery

- Decode kiểm version, type, flags, độ dài, MAX_DATAGRAM, sid và field theo type; DATA tag hợp lệ rồi mới thay đổi state. ACK xác thực vẫn phải kiểm không xác nhận sequence chưa từng phát; malformed ACK không giải phóng buffer hay tăng cwnd.
- DATA seq tối đa `2^32-2`, để ACK kế tiếp `2^32-1` còn biểu diễn được. Dừng/tạo phiên mới trước khi dùng hết seq/stream/counter; không wrap và không reuse nonce. Stream ID 2..65535, thêm trần tài nguyên mục 7.
- Sender giữ cửa sổ sequence 64 tính từ cumulative ACK, ngoài cwnd và credit. Bitmap SACK 32 giữ nguyên; packet xa hơn có thể cần chờ ACK tích lũy hoặc timeout.
- A3 đã dùng 3 sequence phía sau được ACK/SACK mới làm bằng chứng fast retransmit; cùng một ACK lặp không tăng bằng chứng. RTO xử lý tail loss và loss retransmit; không lấy RTT từ retransmit. V2 giữ quy tắc này sau bước xác thực control.
- Recovery boundary đã có ở A3 như mục 3.1: snapshot highest-sent theo từng stream, một fast-loss reduction và tối đa một timeout escalation trong epoch. V2 thêm kết thúc phần snapshot khi stream reset hợp lệ. Retransmit có ngân sách riêng để tiến triển dù inflight>=cwnd. Backoff theo packet có trần, không nhân RTO theo số packet trong batch.
- A4 triển khai CUBIC với clock monotonic inject được; giữ AIMD làm baseline. Chi tiết công thức/tham số và nguồn đặc tả được bổ sung cùng A4; A1 không tuyên bố đã có thuật toán CUBIC.

## 7. Giới hạn và scheduling dự kiến

Giá trị khởi đầu để triển khai/test, không phải số đo RAM hay yêu cầu rubric. MiB=1048576 byte, KiB=1024 byte. Thay đổi phải cập nhật spec/test, không nới riêng một phía. V2 dùng cùng profile cố định, chưa thương lượng profile.

| Tài nguyên | Trần dự kiến |
|---|---|
| Connection/Endpoint server | 64 tổng, gồm chưa auth; B đặt auth timeout 10s |
| Stream mỗi chiều/connection | 32 active (gồm control/chat), 1024 ID từng dùng kể cả tombstone; hết thì phiên mới |
| Message control/chat | 64 KiB |
| Message file (gồm BEGIN/chunk) | 16 KiB; chunk B hiện 8192 byte |
| Pending + unacked payload | 256 KiB/stream, 2 MiB/connection |
| Receive credit ban đầu | 256 KiB/stream, 2 MiB/connection |
| Số message pending hoặc nhận chưa consume | 1024/stream, 4096/connection |
| Reorder | 64 packet/stream; nằm trong receive byte budget |
| Wire cache | chỉ packet unacked; accounting riêng, không giữ lịch sử ACK |
| ACK/control không phải DATA | không bị data credit chặn; coalesce ACK/credit theo stream, không queue vô hạn |

Message cap nhỏ hơn receive window ban đầu để receiver không chờ EOM mãi trong khi sender chờ credit. Empty message tính 0 payload byte nhưng vẫn chiếm message slot/packet; khi vượt slot đã dành thì không ACK dữ liệu không thể giữ. Có thể hồi phục bằng retransmit sau consume. Test flood message rỗng riêng; giới hạn byte không đủ bảo vệ metadata.

Dành 128 KiB connection capacity/credit cho DATA stream 0/1: file chỉ sử dụng 2 MiB-128 KiB chưa consume tại một thời điểm. `conn_limit = 2 MiB + total_consumed`; `file_conn_limit = (2 MiB-128 KiB) + file_consumed`; `stream_limit = 256 KiB + stream_consumed`. Sender kiểm cumulative unique bytes mới của stream, connection và file class trước khi phát. Theo dõi riêng control/chat/file usage khi cấp limit để stream file không chiếm reserve. Phần cấp trước chưa dùng cũng là nghĩa vụ giữ chỗ; tổng grant connection phải được bảo đảm, không chỉ nhìn bytes đã tới. Mỗi chiều theo dõi ledger của từng stream và tổng connection; bộ nhớ toàn endpoint bị chặn thêm bởi trần 64 connection.

Scheduler dùng vòng weighted round-robin có con trỏ giữ qua các pump: tối đa 4 DATA control/chat rồi một lượt DATA file nếu file đủ điều kiện. File round-robin; không có chat thì file dùng phần rỗi, không có file thì chat dùng phần rỗi. ACK/credit/control bảo trì có ngân sách/coalescing riêng; retransmit tuân recovery budget. Đo latency và khả năng file tiến triển bằng test, không suy từ tên scheduler.

## 8. TCP bootstrap A2 — đã có module, chờ B1 tích hợp

API tại `transport/bootstrap.py`: `BootstrapServer(host, tcp_port, udp_port, version=PROTOCOL_VERSION, *, timeout=3.0)` với `await start()`/`await stop()` và `local_addr`; `await bootstrap_connect(server_addr, *, version=PROTOCOL_VERSION, timeout=3.0) -> (peer_ip, udp_port)`. B gọi bootstrap trước UDP connect ở startup/reconnect, stop listener khi dừng server. Hiện PROTOCOL_VERSION=1; chưa bật wire v2.

Framing JSON UTF-8 một dòng, tối đa 4096 byte gồm newline, không giả định một read là một message. Request hiện tại `{"type":"BOOTSTRAP","version":1}`; response `{"type":"BOOTSTRAP_ACK","version":1,"udp_port":9000}`. Đổi version đồng bộ với UDP khi triển khai v2. Validate type, integer version (không nhận bool), udp_port 1..65535; timeout connect/read, EOF sớm/JSON lỗi/oversize đều thất bại và đóng writer. TCP ACK ở đây là message ứng dụng, khác ACK của hệ điều hành.

Client raise `BootstrapError(ConnectionError)` với thuộc tính `.reason`: timeout, connection_failed, unexpected_eof, frame_too_large, invalid_frame, invalid_response, version_mismatch. Sai cấu hình local (port/version/timeout) raise ValueError. Timeout 3 giây tính cho toàn bộ connect + request drain + đọc frame; cleanup writer có timeout riêng tối đa 0,5 giây. Task cancellation vẫn propagate CancelledError và đóng writer. Không fallback UDP khi bootstrap thất bại.

Server dùng timeout cho cả nhận request và drain ACK. Request không hợp lệ/sai version/oversize/EOF/timeout bị đóng, không nhận ACK thành công; client có thể thấy unexpected_eof thay vì reason chi tiết của server. Stop idempotent, đóng listener và mọi accepted writer, cancel rồi await handler tasks. Có thể start lại sau stop; start hai lần khi đang chạy raise RuntimeError. `local_addr` chỉ đọc khi chạy; tcp_port=0 hỗ trợ test, udp_port phải là cổng UDP đã bind thật (1..65535).

Host UDP là **IP peer TCP đã kết nối**, không resolve hostname lại hoặc tin một host tùy ý từ response. B dùng cùng số cổng TCP/UDP 9000 trong cấu hình mặc định; constructor nhận tường minh cả hai. Sau ACK đóng TCP; chat/file tiếp tục UDP. Module bootstrap không tự auth. B phụ trách reconnect và thông báo UI.

Test module: `tests/test_bootstrap.py`; có test harness nối TCP → UDP → login → DM bằng app hiện tại, chưa thay đổi lifecycle production. [Bàn giao và kết quả A2](A2-bootstrap.md). B1 phải nối vào ứng dụng rồi kiểm đường chạy thật trước khi nghiệm thu toàn bộ Step 2.

B cần review các điểm: receipt riêng BEGIN/chunk; chuyển receive sang async handler+consume; relay queue có trần; reset theo chiều gửi; lỗi/stats; auth timeout và bootstrap lifecycle. Đây là hợp đồng bàn giao của A, chưa ghi nhận B đã tích hợp hoặc đồng ý. Thay đổi sau review phải cập nhật hợp đồng và test liên quan.
