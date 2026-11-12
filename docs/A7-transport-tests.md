# A7 — Kiểm thử transport và bằng chứng

Phạm vi theo WORKPLAN A7 và pipeline §15: hoàn thiện hai test skip thuộc A, chứng minh HoL bằng drop đích danh, kiểm receiver chậm/disconnect/cleanup và bàn giao simulator cho B6. A7 bổ sung test và tài liệu trên implementation A6; không đổi wire v2, API hoặc cấu hình production.

## 1. Mốc trước A7 và thay đổi

Log [A6-full-suite.txt](evidence/A6-full-suite.txt) ghi **251 passed, 4 skipped**. Đây là kết quả A6 đã lưu, không phải một lượt baseline mới chạy trong A7. Hai stub HoL/disconnect nay có thân test và assertion thật; mỗi test chạy hai trường hợp, thay vì chỉ xóa decorator skip.

| File | Thay đổi A7 |
|---|---|
| `tests/test_reliability.py` | HoL trên CUBIC/AIMD; disconnect do retry/timeout; loss 5%/10%; kiểm thứ tự/nội dung/ACK; log và cleanup khi assertion thất bại |
| `tests/test_flow_control.py` | Mở rộng test receiver chậm: chat khi producer bị chặn, consume tiếp hoặc stop; kiểm receipt/task/buffer |
| `docs/protocol.md` | Cập nhật bằng chứng HoL và liên kết A7 |
| `docs/A6-security-resilience.md` | Liên kết kết quả A7 tiếp nối, giữ nguyên kết quả lịch sử A6 |

`LossyLink` đã có đủ drop hook, event recording và cancel delayed handles từ A3; A7 dùng lại. Không cần sửa module production hoặc thêm dependency.

## 2. HoL: giữ packet file cho đến khi chat đã đến

`test_no_head_of_line_blocking[algorithm]` chạy UDP loopback, mã hóa/ACK/timer thật, với `algorithm=cubic` và `aimd`:

1. Enqueue bốn message file, mỗi message 8192 byte, trên stream 5.
2. Drop DATA `stream_id=5, seq=0`, gồm cả retransmit, khi Event `release` chưa được bật.
3. Chờ quan sát ít nhất hai lần drop và xác nhận chưa giao file cho receiver.
4. Gửi chat trên stream 1. Trong deadline 2s, chat phải được giao đúng nội dung, trong khi `release` vẫn tắt, file vẫn chưa được giao và seq 0 vẫn unacked.
5. Bật `release`; chờ ACK file trong 5s. Kiểm đủ bốn message theo đúng thứ tự, không trùng, cache/pending/unacked về 0.
6. Các lần gửi seq 0 phải có datagram giống hệt nhau, gồm ciphertext.

Điểm chứng minh là **chat được giao trước khi cho phép packet file thiếu đi qua**. Không dùng loss ngẫu nhiên hoặc chỉ so sánh hai thời gian hoàn tất. cwnd vẫn dùng chung giữa các stream; kết quả không có nghĩa chat miễn ảnh hưởng của congestion. Deadline 2s là giới hạn phát hiện treo trong test, không phải cam kết latency sản phẩm. Test latency riêng của A5 vẫn dùng p95 < 0,25s trên loopback.

## 3. Disconnect: tách nguyên nhân và xác nhận server còn sống

`test_peer_disconnect_detected[cause]` dùng Endpoint và timer thật, monkeypatch hằng số **chỉ trong test**:

| Cấu hình | `retries` | `timeout` |
|---|---|---|
| Packet bị chặn sau handshake | DATA từ server tới connection đích | Mọi packet của connection đích, cả hai chiều |
| MAX_RETRIES | 2 | 100 |
| INITIAL_RTO / MIN_RTO / MAX_RTO | 0,05 / 0,02 / 0,1s | 5 / 5 / 5s |
| PEER_TIMEOUT | 5s | 0,5s |
| Heartbeat interval | 0,05s | 0,05s |
| Reason bắt buộc | `too many retries` | `timeout` |

Hai message 16 byte lấp đầy ngân sách stream 32 byte. Hai tác vụ chờ ACK và một tác vụ gửi thêm phải thật sự pending trước khi connection đóng. Ở nhánh timeout, RTO dài hơn peer timeout nên vẫn kiểm được waiter mà không để hết retry gây đóng trước.

Assertion chung:

- Disconnect trong 3s, callback đúng một lần và đúng reason; gọi tick/close lần nữa không phát callback lặp.
- Cả ba waiter kết thúc bằng `ConnectionClosed` đúng reason, trong deadline riêng 1s.
- Pending/unacked/receipt/wire cache/reassembly/control/retransmit/reset/probe được dọn; slot connection và cache HELLO được trả.
- Nhánh retry vẫn nhận heartbeat hợp lệ và seq 0 được gửi đúng một lần đầu cộng MAX_RETRIES lần gửi lại.
- Nhánh timeout không retransmit DATA, `last_recv` không tiến triển.
- Server cấu hình `max_connections=1`; sau disconnect, client mới handshake, gửi chat và nhận ACK thành công. Điều này kiểm cả giải phóng capacity và khả năng phục vụ tiếp.

Đây là timeout của transport. Dọn username, gửi USER_LEFT, file/app cleanup và reconnect vẫn thuộc B4/B6.

## 4. Receiver chậm và cleanup

`test_real_udp_producer_waits_for_slow_consumer[consume|stop]` dùng profile nhỏ để đạt giới hạn nhanh:

- Receiver giữ 32 byte file chưa consume; sender có 32 byte pending và producer chưa kết thúc.
- Chat vẫn được giao và ACK trong lúc gate file đóng, producer còn bị chặn.
- Nhánh `consume`: mở gate, nhận đúng 10 message theo thứ tự, tất cả receipt được ACK và receive buffer về 0.
- Nhánh `stop`: dừng receiver khi đang chặn; producer và receipt chưa ACK báo `ConnectionClosed`; consumer task kết thúc, queue và buffer được dọn.

Helper `_pair` của reliability dùng AsyncExitStack đăng ký cleanup trước khi start, nên một endpoint lỗi không làm bỏ qua cleanup của endpoint còn lại. Cleanup hủy delayed delivery, tháo link rồi stop với deadline 2s; kiểm timer, consumer, socket, connection/cache và pending handshake. Tháo link trước stop tránh CLOSE tạo delayed delivery mới sau `link.close()`.

`test_cleanup_cancels_delayed_packets_when_assertion_fails` inject một assertion thất bại sau khi đã schedule hai bản sao packet trễ 60s. Sau thoát context, cả hai handle phải bị cancel, không có delivery, socket đóng và receipt báo lỗi. Test không chờ 60s và chỉ bắt AssertionError có thông điệp inject đã định trước.

## 5. Loss, số liệu và giới hạn đo

`test_delivery_with_loss` gửi 200000 byte thành các message có nội dung phân biệt được, kiểm danh sách message chính xác và tổng payload được ACK. Hai chiều được gắn link riêng với cùng profile:

| Profile | Loss | Delay | Jitter | Duplicate | Seed mỗi chiều |
|---|---:|---:|---:|---:|---:|
| loss5-delay-jitter-duplicate | 0,05 | 0,005s | 0,005s | 0,02 | 5 |
| loss10 | 0,10 | 0s | 0s | 0 | 1 |

Log gồm cấu hình, SHA-256, số byte, packet sent/dropped mỗi link, retransmit/timeout/recovery, cwnd cuối và peak lấy mẫu, pending cuối. Lấy mẫu queue mỗi 0,005s và kiểm ngân sách stream/connection; **sampled peak có thể bỏ lỡ đỉnh giữa hai lần lấy mẫu**. Đây không phải RSS toàn process. Kiểm bộ nhớ 10/100 MiB của A5 được chạy trong full suite, dùng tracemalloc và accounting của transport.

Deadline ACK là 20s sau enqueue; chờ delivery và drain delayed handles có deadline riêng. Các ca mất DATA/ACK/gói cuối/lần retransmit từ A3 vẫn được chạy. Ca jitter/duplicate chờ các delayed handle hết trước assertion cuối để kiểm cả duplicate đến muộn.

Seed cố định giúp tái hiện chuỗi RNG, nhưng lịch event loop và ACK có thể làm khác số retransmit giữa các lượt. Jitter tạo khả năng đảo thứ tự, không tương đương một tỷ lệ reorder cố định. Loss 30% là stress tùy chọn trong pipeline, chưa bổ sung ở A7.

## 6. Chạy lại và bằng chứng

Môi trường và base revision được ghi ngay đầu từng log. Working tree chạy test gồm cả thay đổi A6 và A7 chưa commit; base HEAD không phải một commit chứa riêng A7.

Nhóm transport (bỏ riêng phép đo 10/100 MiB để chạy nhanh; full suite vẫn chạy phép đo này):

```powershell
.\.venv\Scripts\python.exe -X utf8 -m pytest tests/test_reliability.py tests/test_recovery.py tests/test_flow_control.py tests/test_endpoint_resilience.py -k "not large_transfer_memory" -q -rs -s -p no:cacheprovider -o faulthandler_timeout=60
```

Kết quả: **118 passed, 1 deselected trong 10,47s**, [A7-transport-tests.txt](evidence/A7-transport-tests.txt).

```powershell
.\.venv\Scripts\python.exe -X utf8 -m pytest -q -rs -s -p no:cacheprovider -o faulthandler_timeout=120
```

Kết quả toàn suite: **258 passed, 2 skipped trong 91,88s**, [A7-full-suite.txt](evidence/A7-full-suite.txt). So với mốc A6: hai skip chuyển thành bốn ca HoL/disconnect chạy thật; thêm một profile loss, một ca cleanup khi assertion lỗi và một nhánh stop của receiver chậm. `faulthandler_timeout` in stack để chẩn đoán khi test kéo dài; không thay thế timeout/assertion của từng kịch bản.

Số liệu của lượt full suite này:

| Kịch bản | Kết quả quan sát |
|---|---|
| HoL CUBIC / AIMD | Chat đến sau khoảng 0,0249 / 0,0310s trong lúc packet file vẫn bị chặn; file 32768 byte hoàn tất sau khi thả packet |
| Retry / peer timeout | Disconnect sau khoảng 0,310 / 0,522s; ba waiter báo lỗi; client mới gửi và nhận ACK thành công |
| Receiver chậm | Sender pending 32 byte, receiver giữ 32 byte theo giới hạn test; chat được ACK trước consume/stop |
| Loss 5% / 10% | Nhận đủ 200000 byte mỗi ca; 19 / 31 retransmit trong lượt này |
| File 10 / 100 MiB của A5 | Tracemalloc peak 743728 / 740568 byte; payload peak sender/receiver đều 262144 byte |

Các phép đo là loopback cùng process trên Windows 11/Python 3.14.4, không đại diện throughput/latency LAN. Không suy ra CUBIC nhanh hơn AIMD từ hai số đo HoL này.

Hai skip còn thuộc B6 theo WORKPLAN:

- `tests/test_auth_routing.py::test_client_crash_is_cleaned_up`.
- `tests/test_file_transfer.py::test_file_over_lossy_network`; nhãn TODO(A) trong stub là nhãn owner cũ. A7 không sửa thân test này hoặc coi nó đã đạt.

## 7. Bàn giao simulator cho B6 và bước tiếp theo

Hook hiện có: `endpoint.link = LossyLink(...)`. Chọn gắn sau handshake nếu chỉ thử loss DATA/control; gắn trước `connect()` nếu muốn cả handshake chịu loss. `drop_rule(packet, attempt)` quyết định drop đích danh, trả False để tiếp tục áp dụng random loss. Có thể lọc bằng `conn_id`, `ptype`, `stream_id`, `seq`. `record=True` giữ event có packet, thời điểm, attempt, dropped và raw wire; chỉ dùng cho kịch bản hữu hạn vì log không có trần.

Đối với relay A → server → B:

1. Dựng server và hai client thật; gắn link ở **cả ba Endpoint**, không chỉ cặp sender/server.
2. `client_a.link` xử lý A → server; `client_b.link` xử lý B → server; `server.link` xử lý cả server → A và server → B (DATA, ACK, credit).
3. Ghi seed/profile của từng link. Một `server.link` dùng chung RNG cho hai địa chỉ: không ghi thành bốn RNG độc lập. Nếu cần profile riêng mỗi chiều đích, B có thể đặt adapter dispatch theo `addr` vào `server.link`, với một LossyLink cho từng địa chỉ; adapter phải forward `send(real_sendto, data, addr)` và đóng tất cả link con khi cleanup.
4. Kiểm file tại receiver cuối đúng size/hash và trạng thái DONE, không chỉ receipt ACK ở chặng đầu. Kiểm lỗi/cleanup khi sender hoặc receiver mất kết nối.
5. Trong finally, đóng link để hủy delayed handle, tháo link và stop các endpoint; không thay loss/backpressure bằng sleep cố định.

Không có CLI `--loss` mới. Không có kiểm thử LAN hai máy hoặc load 20 client trong A7: LAN là J3; app/file/load là B6 và tích hợp J2. A8 tiếp tục protocol/report/diagram dựa trên các log thật này.
