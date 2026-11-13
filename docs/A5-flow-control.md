# A5 — Flow control, bounded buffers và scheduler

Ngày: 11/11/2026 (Asia/Saigon). Revision nền `82e361a39ac0eb47a848e1e5a888c06da8c3e24e` cộng thay đổi A5 chưa commit. Python 3.14.4, Windows 11 build 26200.

## Kết quả

Đã triển khai byte/packet credit theo stream và connection, budget dành riêng control/chat, API gửi có chờ/ACK receipt, nhận async theo stream/consume, reset có retry, stats snapshot và scheduler weighted round-robin. Đã nối producer file và relay hiện có sang API gửi có chờ để không bỏ chunk khi queue đầy.

**Wire version đã tăng lên 2.** Cả client và server phải chạy source mới; không tương thích wire v1. Bootstrap mặc định lấy version=2 từ constants. Khi ghi bằng chứng A5 bên dưới, controller còn AIMD. A4 triển khai sau A5 đã bật CUBIC mặc định, giữ wire v2; kết quả mới và cách chọn AIMD ở [A4-cubic.md](A4-cubic.md). Sau đó [A6](A6-security-resilience.md) bổ sung validation, cap connection và kiểm thử crypto/lifecycle. Việc chung J1/J2 vẫn cần nghiệm thu riêng.

## File và phạm vi

| File | Vai trò |
|---|---|
| `transport/flow.py` | FlowLimits, receipt, delivery token, exception và immutable stats |
| `transport/flow_control.py` | Ledger credit, queue budget, consume, scheduler và reset |
| `transport/connection.py` | Nối credit vào DATA/recovery; authenticated control, replay và budget |
| `transport/endpoint.py` | API async, dispatcher theo stream, peer/stats accessor, close/stop waiter cleanup |
| `transport/stream.py` | Receipt mapping, byte accounting, fragment cap và stream tombstone |
| `transport/crypto.py` | DATA/control nonce domain v2, HKDF version binding |
| `common/constants.py`, `packet.py` | Version/type mới, datagram/type validation |
| `client/client.py` | Producer await BEGIN/chunk; bỏ sleep thay backpressure |
| `server/server.py`, `router.py` | Async receive/relay, consume sau bounded enqueue; lấy peer qua API |
| `tests/test_flow_control.py` | 27 case A5 gồm slow receiver, reset, memory và chat latency |
| `tests/test_recovery.py`, `test_reliability.py` | Cập nhật test cho authenticated controls và message cap, giữ assertion recovery |

Adapter app là phần tối thiểu cần cho backpressure; B cần review cùng B3. Message/file state, upload streaming, FILE_ERROR, cleanup file handle và tiến độ UI chưa được coi là hoàn thành bởi task này.

## API bàn giao cho B

```python
from transport.flow import ConnectionClosed, MessageTooLarge, StreamCancelled

# Một lần gọi = một message, không enqueue một phần.
receipt = await endpoint.send_message(conn_id, stream_id, chunk)
# Return nghĩa queue đã nhận ownership; không phải receiver đã lưu file.
print(receipt.message_id, receipt.payload_bytes, receipt.acked_bytes)
await receipt.wait_acked()  # Khi cần xác nhận riêng message này đã ACK.

stats = endpoint.get_stats(conn_id)
peer_ip, peer_port = endpoint.get_peer_addr(conn_id)

# Chỉ hủy chiều gửi cục bộ, sid>=2; chờ peer xác nhận reset.
await endpoint.cancel_stream(conn_id, stream_id, reason="transfer cancelled")
```

Producer không cần chờ ACK từng chunk tuần tự; có thể tiếp tục await enqueue chunk kế tiếp để tận dụng cửa sổ, theo dõi receipt khi cần progress. B chỉ đếm receipt payload chunk, loại FILE_BEGIN/header/tag. 100% ACK chặng client→server vẫn chờ FILE_DONE từ receiver để xác nhận thành công cuối cùng.

Receive async phải cấu hình trước start, không đồng thời dùng on_receive sync:

```python
async def receive(message):
    # Ví dụ tại server: ánh xạ stream đích bằng file state của ứng dụng.
    await destination_endpoint.send_message(dst_conn, dst_stream, message.data)
    endpoint.consume(message)

endpoint.set_message_handler(receive)
```

Một worker/stream, FIFO trong stream; chat và file xử lý độc lập. Consumer chỉ consume sau xử lý hoặc chuyển ownership sang queue có trần. Không create_task cho từng chunk rồi consume ngay. Token consume idempotent, thuộc connection/identity cụ thể; token đã dọn là no-op. Handler return chưa consume vẫn giữ credit. Handler lỗi file yêu cầu peer stop/reset; handler lỗi control/chat đóng connection.

Sync on_receive vẫn hỗ trợ và tự consume sau callback return: callback phải xử lý xong dữ liệu hoặc dùng buffer app có quản lý; không dùng mode này để enqueue relay vô hạn. Callback sync lỗi đóng connection; async mode hỗ trợ reset riêng file. Legacy send trả False khi queue đầy; nơi nào cần delivery phải chuyển sang await send_message và xử lý exception.

Cancellation trước enqueue không phát dữ liệu. Hủy task wait_acked không thu hồi DATA. cancel_stream fail các receipt còn chờ bằng StreamCancelled; close/stop đánh thức send/ACK/reset/handshake waiter bằng ConnectionClosed, dọn dispatcher tasks. Reset không hủy stream cùng số ở chiều ngược lại.

Stats là dataclass frozen với breakdown tuple per-stream, byte/packet units rõ ràng, không lộ mutable Connection. Chỉ get_stats khi connection còn trong Endpoint; lưu snapshot/reason qua disconnect nếu UI cần hiển thị sau đóng. StreamStats.cancelled phản ánh một trong hai chiều của sid đã reset; byte fields phân biệt chiều gửi và nhận.

## Budget và wire v2

- Payload pending+unacked tối đa 256 KiB/stream, 2 MiB/connection; dành 128 KiB cho stream 0/1. Queued message tối đa 1024/stream, 4096/connection, có message-slot reserve cho control/chat.
- Receive credit ban đầu 256 KiB/stream và 2 MiB/connection; file-class connection credit là 2 MiB-128 KiB. Grant chỉ tăng khi consume, không khi ACK.
- Thêm packet-slot credit: 1024/stream, 4096/connection, reserve 128 cho control/chat. DATA rỗng vẫn tốn một slot. Slot giữ tới khi message được consume, bao gồm reorder/fragments/dispatcher/handler đang chạy.
- Message cap: 64 KiB control/chat, 16 KiB file. FILE_CHUNK_SIZE vẫn 8192. Buffer thiếu EOM tới khi vượt cap đóng protocol_error. Message hợp lệ phải vừa initial byte/packet credit để không deadlock trước EOM.
- 32 active stream mỗi chiều, trong đó giữ chỗ cho control/chat; 1024 stream ID từng dùng mỗi chiều, gồm tombstone. Không reuse ID. B cần release file stream bằng cancel/reset sau transfer kết thúc khi không còn cần stream đó, đồng thời dọn file state riêng.
- ACK chỉ phát sau khi DATA được giữ trong ngân sách; không ACK rồi vứt vì queue đầy. Duplicate ACK/DATA và retransmit không tăng tiến độ hoặc trừ credit lần hai.
- CREDIT body 6 uint64: stream/connection/file-class limit tính theo byte, rồi ba limit tính theo DATA packet-slot. Sender lấy max, không cộng delta. Lost update phục hồi bằng CREDIT_PROBE với RTO/backoff tối đa 5s.
- RESET body final_bytes + final_packets + reason. Final chỉ tính DATA đã phát, không pending. Retry reset/ACK dùng counter mới; receiver trả credit phần đã nhận/thiếu đúng một lần, giữ tombstone chặn DATA tới muộn. STOP_STREAM cho handler file lỗi yêu cầu peer cancel.
- Control AES-GCM có counter 64 bit/chiều, replay bitmap 256, nonce domain riêng DATA. HKDF info v2. Tamper/replay không cập nhật credit/liveness. HELLO vẫn chưa xác thực danh tính; không tuyên bố chống MITM.
- DATA scheduler: tối đa 4 lượt control/chat rồi một lượt file đủ điều kiện; file cursor giữ qua các pump. Không có chat thì file dùng phần rỗi. ACK/credit/control độc lập DATA credit, coalesce theo type/sid, budget 16 frame mỗi tick. Retransmit giữ pacing/recovery của A3.

Chi tiết wire và API: [protocol.md](protocol.md). FlowLimits là profile cố định: không thay riêng một phía. Custom profile trong test luôn dùng giống nhau hai phía, chưa có negotiation qua handshake.

## Bằng chứng

Test nhanh (bỏ stress 10/100 MiB):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_flow_control.py -q -s -p no:cacheprovider -k "not large_transfer"
```

Lượt test nhanh cuối: **26 passed, 1 deselected in 0.57s**, exit 0. [Raw stdout](evidence/A5-quick-tests.txt). Có thêm regression test callback đóng connection giữa một batch reassembly: không giao các message còn lại và không giữ delivery token sau close.

Test đủ A5 gồm stress 100 MiB; có thể mất vài chục giây đến vài phút tùy tải máy:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_flow_control.py -q -s -p no:cacheprovider
```

Toàn suite đã chạy:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -rs -s -p no:cacheprovider
```

Kết quả cuối **1 failed, 114 passed, 4 skipped in 294.30s**, exit 1. [Raw stdout](evidence/A5-full-suite.txt). Cả 27 case A5 đạt. Failure vẫn là test_login_broadcast_private_end_to_end/assertion broadcast dòng 60 đã có từ A1; test chờ số lượng event chung thay vì đúng chat event. Các test A2 và file relay hiện có đạt; bốn skip A7/B6 vẫn giữ. Đây là lượt full suite sau thay đổi cuối về reason local của cancel và regression close-callback.

Số đo trong lượt full suite đã lưu:

| Transport payload | tracemalloc peak | Send payload peak | Receive payload peak | Kết quả |
|---|---:|---:|---:|---|
| 10 MiB | 743184 byte | 262144 byte | 262144 byte | Consume đủ 10485760 byte, SHA-256 đúng |
| 100 MiB | 739808 byte | 262144 byte | 262144 byte | Consume đủ 104857600 byte, SHA-256 đúng |

Đây là cặp Connection mã hóa thật, queue mạng mô phỏng và consumer chậm một chunk/lượt, không giữ trace toàn payload. tracemalloc đo allocation Python trong bài thử, không phải RSS toàn app hay browser/upload. Tiêu chí đặt trước: cả hai peak <16 MiB và peak 100 MiB không vượt peak 10 MiB thêm 2 MiB.

| Chat loopback (20 sample mỗi lượt) | p50 | p95 | Ngưỡng nội bộ đặt trước |
|---|---:|---:|---:|
| Không có file | 0.996 ms | 2.493 ms | p95 <250 ms |
| File consumer giữ đủ stream credit | 0.964 ms | 4.119 ms | p95 <250 ms |

Đo từ trước enqueue tới callback nhận chat cùng process, không phải latency LAN. Chat tới trong khi file consumer vẫn đang chờ gate; không kết luận latency không đổi trên mọi mạng. Scheduler unit test còn kiểm chính xác chuỗi 4 chat : 1 file và con trỏ qua nhiều pump.

Các test còn kiểm lost credit/probe, replay/reorder credit, duplicate DATA, byte/packet reserve, message rỗng, close/cancel waiter, reset/ACK mất gói, reset accounting DATA chưa tới, exhausted reset retry, handler lỗi, stop từ handler/đang handshake, token giả và fragment thiếu EOM.

Kiểm tài liệu/code: AST parse đạt cho các file thay đổi chính; ví dụ header 23 byte khớp encoder; local link không hỏng; git diff --check đạt. Log được lưu UTF-8. [Sơ đồ backpressure](diagrams/flow_control_a5.md).

## Phần còn lại

- B3/B4 nối ACK receipt với file ID/UI, FILE_ERROR và cleanup receiver/sender task/file handle. Progress hiện tại của app vẫn đếm enqueue; A5 chỉ cung cấp API ACK đúng nghĩa.
- B5 giới hạn upload HTTP/UI event queue; số đo memory transport không bao gồm các buffer app này.
- A4 đã bổ sung CUBIC sau mốc bằng chứng A5; A6 đã thêm validation handshake, cap connection mặc định 64 và xử lý close UDP còn pending trên Windows. Chưa có negotiation profile hoặc xác thực danh tính trong handshake. Các log A5 vẫn là bằng chứng tại thời điểm chạy A5.
- A7 hoàn thiện HoL loss đích danh và peer disconnect; B6 file/hash qua loss cả hai chặng và load 20 app client. A5 không thay thế các phép thử này.
- Test sạch/LAN, J1/B review API và các bước nộp bài vẫn theo WORKPLAN.
