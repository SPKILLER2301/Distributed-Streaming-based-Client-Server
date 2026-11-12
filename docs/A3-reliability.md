# A3 — Reliability: fast retransmit, recovery và timeout

Ngày: 10/11/2026 (Asia/Saigon). Revision nền `5ae125ea63d5a276899327cf3b900569f441cc80` cộng thay đổi A3 chưa commit. Python 3.14.4, Windows 11. Theo WORKPLAN A3 và pipeline §8.

## Kết quả và phạm vi

Đã hoàn thiện reliability trên UDP wire v1: ACK validation, cửa sổ sequence gửi, distinct-SACK fast retransmit, recovery epoch theo connection với boundary từng stream, deadline/backoff từng packet, giới hạn lượt retransmit, retry exhaustion và cleanup. DATA retransmit dùng nguyên bytes mã hóa ban đầu. Không đổi API Endpoint hoặc bật wire v2.

Trước A3, sender chỉ gửi lại theo RTO chung, chưa fast retransmit, chưa kiểm ACK đầy đủ, không giữ cửa sổ sequence theo cumulative ACK, gửi toàn batch timeout và mã hóa lại DATA. Các hành vi này đã được thay bằng quy tắc ở [protocol §3](protocol.md#3-điều-khiển-luồng--tắc-nghẽn--gửi-lại).

## File đã thay đổi

| File | Hành vi mới |
|---|---|
| `transport/stream.py` | highest_sent/cumulative_ack; validate ACK trước mutation; gap evidence chỉ tính ACK mới; deadline/interval; chặn seq wrap |
| `transport/connection.py` | clock inject được; hàng đợi retransmit deduplicate; recovery; pacing một retransmit mỗi 20ms; cache DATA; counters; close dọn buffer |
| `transport/congestion.py` | Ghi rõ AIMD baseline/A4 CUBIC; Connection không gọi backoff RTO chung nữa |
| `tests/lossy_socket.py` | drop_rule(packet,attempt), trace opt-in và close() hủy delayed delivery |
| `tests/test_recovery.py` | 25 test case deterministic bằng clock giả và cặp Connection mã hóa thật |
| `tests/test_reliability.py` | 4 test case drop đích danh qua UDP thật; nâng assertion và finally cleanup cho test hiện có |
| `docs/protocol.md` | Cập nhật hành vi đã triển khai, phân biệt với hợp đồng A4–A6 |

## Các quyết định cần biết khi làm A4/A5

- ACK phải nằm trong phạm vi **đã phát**, không chỉ phạm vi đã enqueue. Một SACK invalid khiến cả ACK bị bỏ trước khi xóa unacked/cộng progress. ACK cũ hợp lệ vẫn có thể thêm SACK, nhưng cumulative_ack không giảm.
- Packet mới thỏa cả inflight<cwnd và seq<cumulative_ack+64. Giữ bitmap SACK 32 bit hiện tại; phần window ngoài bitmap chờ ACK tích lũy hoặc timeout.
- Mỗi sequence được xác nhận chỉ góp bằng chứng một lần. Ba sequence khác nhau cao hơn packet thiếu kích hoạt fast retransmit một lần cho packet đó; mất bản fast retransmit dùng RTO.
- Recovery snapshot highest_sent của từng stream có unacked. Không tăng cwnd khi xử lý ACK trong recovery. Có một lần giảm theo fast loss và tối đa một lần escalation theo timeout; không giảm cho từng packet hết hạn. Kết thúc epoch khi toàn bộ dữ liệu trong snapshot đã ACK, kể cả còn DATA mới hơn chưa ACK.
- Packet giữ RTO snapshot lúc phát. Deadline/interval không thay đổi khi một packet khác cập nhật SRTT. Timeout retransmit nhân đôi interval riêng tới MAX_RTO; fast retransmit chỉ đặt lại deadline. RTO nền không bị nhân thêm một lần ở connection.
- Một slot retransmit cho toàn connection mỗi TICK_INTERVAL, không bù burst khi tick trễ. Retransmit vẫn phát được dù inflight>=cwnd; gửi mới vẫn chịu cwnd. Đây là pacing recovery đơn giản, chưa phải scheduler ưu tiên/chat/file của A5.
- Sequence DATA dừng ở 2^32-2, dành giá trị cuối cho ACK. Enqueue vượt giới hạn không thêm packet một phần, đóng connection trước wrap.
- Cache wire DATA được thực hiện sớm ở A3 vì retransmit cần bất biến ciphertext/header. A6 tiếp tục làm authenticated control/domain nonce, A5 tính cache vào ngân sách bộ nhớ.

## Kiểm thử thực tế

Chạy focused và xuất counters:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_recovery.py tests/test_packet.py tests/test_reliability.py -q -rs -s -p no:cacheprovider
```

Kết quả: **40 passed, 2 skipped in 6.77s**, exit 0. [Raw stdout và counters](evidence/A3-transport-tests.txt).

| Kịch bản UDP thật | DATA mới | Fast retransmit | Timeout retransmit | Kết quả |
|---|---:|---:|---:|---|
| Mất DATA seq 1 lần đầu | 8 | 1 | 0 | Đúng thứ tự, không trùng, ACK đủ 8 byte |
| Mất lượt ACK đầu cho mỗi identity | 8 | 0 | 1 | ACK lại khi DATA trùng; app không giao trùng |
| Mất packet cuối seq 7 | 8 | 0 | 1 | Tail loss phục hồi bằng RTO |
| Mất DATA seq 1 và bản gửi lại đầu | 8 | 1 | 1 | Escalation fast → timeout, nội dung đúng |
| Loss 10% hai chiều, seed=1 mỗi chiều | 173 | 15 | 10 | 200000 byte nhận/ACK đủ, pending cuối=0 |

Lượt loss 10% cuối có cwnd=4.168140074025552 packet, 8 recovery epoch, 6 loss reduction và 4 timeout reduction. Các số là kết quả lượt chạy đã lưu, không phải giá trị bắt buộc của test. Seed cố định nhưng lịch event loop có thể làm số retransmit khác giữa máy/lượt. Delay/jitter/duplicate=0 ở kịch bản loss 10%; test reorder riêng dùng jitter=0.03s, duplicate=0.1, seed=2 mỗi endpoint.

Test clock giả kiểm thêm: ACK invalid không mutation, stale ACK, enqueued≠sent, SACK không mở quá cửa sổ, nhiều lỗ hổng, boundary hai stream, packet mới không kéo dài recovery cũ, RTO snapshot riêng, batch timeout không burst, backoff 1→2→4→5→5 giây, inflight>cwnd vẫn retransmit, ACK dọn hàng chờ, Karn, wire bytes bất biến, hết retry và cleanup/callback một lần, seq overflow. Unit retry test kiểm soát thời gian để không nhầm với peer timeout.

Toàn suite:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -rs -p no:cacheprovider
```

Kết quả: **1 failed, 87 passed, 4 skipped in 14.00s**, exit 1. [Raw stdout](evidence/A3-full-suite.txt). Các test A2 trong full suite đạt. Fail vẫn là `test_login_broadcast_private_end_to_end`, assertion broadcast dòng 60, cùng failure đã ghi ở A1/A2. Không sửa test app trong A3; B6 cần kiểm predicate chờ đúng chat thay vì số lượng event chung.

Hai skip transport là `test_no_head_of_line_blocking`, `test_peer_disconnect_detected` thuộc A7. Hai skip app/file thuộc B6. Test retry exhaustion A3 đã chạy nhưng chưa thay thế test peer disconnect của A7. Không bỏ skip khi chưa có thân test đúng yêu cầu.

## Giới hạn và bàn giao

- Exactly-once là không giao trùng DATA/message trong cùng connection/stream; không bảo đảm qua reconnect hoặc app callback lỗi sau ACK.
- ACK/control v1 vẫn chưa có xác thực mật mã. Validation phạm vi ACK không thay xác thực; A6 xử lý control giả/replay và liveness.
- Pending/fragments chưa có trần byte/message hoặc receive credit; A5 xử lý. Stream ID/connection cap và stop handshake/tick lifecycle còn thuộc A6. Chỉ seq wrap được chặn sớm trong A3.
- Payload loss test là message trên stream file 5, không phải file application qua server relay hai chặng. B6 vẫn cần test file/hash end-to-end qua loss cả hai chặng.
- Không có số đo LAN, RAM, p50/p95 hoặc fairness; không suy ra các kết luận đó từ pass.
- B không cần đổi call site cho A3. A4 thay thuật toán congestion qua on_ack/on_loss/on_timeout, giữ recovery boundary ở Connection. A5 thêm flow control/scheduler/receipt trên state A3 này.

## Nghiệm thu A3

- [x] Fast retransmit dựa trên SACK của sequence khác nhau, ACK lặp không kích hoạt lại.
- [x] Per-packet exponential backoff có trần, recovery epoch và ngân sách retransmit.
- [x] Test mất DATA/ACK/packet cuối/retransmit, reorder/duplicate, hết retry.
- [x] Giao đúng thứ tự, không trùng; ACK bytes không cộng retransmit; close cleanup.
- [x] Log seed/counters và tài liệu khớp code; toàn suite được chạy và failure nền được ghi rõ.
