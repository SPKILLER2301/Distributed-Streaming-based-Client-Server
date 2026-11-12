# A4 — CUBIC và tích hợp recovery/credit

Ngày triển khai: 11/11/2026, sau A5. CUBIC là mặc định cho mọi Connection tạo qua Endpoint, gồm đường chạy server/client hiện tại. AIMD được giữ làm baseline. Wire tiếp tục là v2 của A5; A4 không thêm packet hoặc sửa crypto/handshake.

## 1. Phạm vi và nguồn

Nguồn thuật toán: [RFC 9438 — CUBIC for Fast and Long-Distance Networks](https://www.rfc-editor.org/rfc/rfc9438.html), tháng 8/2023, thay RFC 8312. Code tham chiếu các mục 4.2–4.8; dùng C=0,4 và beta=0,7. Có tăng cửa sổ theo ACK/thời gian, vùng Reno-friendly, fast convergence và restart sau timeout.

Đây là bản thích nghi cho transport UDP của project: cửa sổ và ACK accounting tính theo **packet DATA**, gồm cả packet ngắn/rỗng. Không quy đổi payload thành SMSS bytes; vì vậy không tuyên bố byte fairness tương đương TCP khi kích thước payload khác nhau. Giữ Reno slow start phù hợp phạm vi bài tập; chưa có HyStart++, ECN, khôi phục trạng thái sau spurious loss hay pacing cho toàn bộ DATA. Các lựa chọn này được nêu để phân biệt phạm vi thực tế với một TCP stack đầy đủ.

## 2. File và cấu hình

| File | Thay đổi A4 |
|---|---|
| `transport/congestion.py` | `CubicController`, factory, giao diện chung; giữ `CongestionController` AIMD cũ |
| `transport/connection.py` | Clock/RTT/flight cho controller, đánh dấu flight dùng hết cửa sổ, recovery và log |
| `transport/stream.py` | Cờ nội bộ `InFlight.cwnd_limited`; không thay packet wire |
| `transport/endpoint.py` | Chọn controller, kiểm cấu hình trước mở socket |
| `transport/flow_control.py` | Stats ghi algorithm thật; cập nhật trạng thái sử dụng cửa sổ sau cancel |
| `tests/test_congestion.py` | Vector thời gian giả, tích hợp, các kịch bản cố định |
| `tests/test_recovery.py` | Chạy các ca recovery quan trọng với cả AIMD/CUBIC |
| `tests/congestion_scenario.py` | Chạy so sánh tái lập, xuất JSON và CSV |

```python
from transport.endpoint import Endpoint

server = Endpoint(is_server=True)  # cubic mặc định
client = Endpoint(congestion_algorithm="cubic")
baseline = Endpoint(congestion_algorithm="aimd")
# Sau connect:
# snapshot = client.get_stats(conn_id)
# print(snapshot.algorithm, snapshot.cwnd_packets)
```

Lựa chọn có hiệu lực cho các connection của Endpoint đó. Hai đầu có thể chọn khác nhau: mỗi đầu tự quản lý chiều gửi của mình. Không cần negotiation thuật toán. Tên sai gây `ValueError` ngay khi khởi tạo Endpoint. Mỗi connection có controller riêng.

## 3. Cửa sổ và đồng hồ

- `cwnd`, `ssthresh`, `inflight`, `W_max`, `W_est` tính bằng packet. `cwnd` khởi tạo 10, giới hạn 1..64; ngân sách DATA mới là `floor(cwnd)`.
- Slow start tăng 1 mỗi packet mới được ACK đủ điều kiện, dừng ở `ssthresh`. Từ ngưỡng này bắt đầu congestion avoidance.
- Trạng thái chính gồm `w_max`, `cwnd_prior`, `cwnd_epoch`, `w_est`, `k`, `epoch_start`. Thời gian là monotonic seconds, inject được cho test.
- Đường cong: `W_cubic(t) = 0.4 * (t - K)^3 + W_max`; `K = cbrt((W_max - cwnd_epoch) / 0.4)`. Dùng căn bậc ba có dấu để đường cong liên tục nếu fast convergence đưa W_max xuống dưới cửa sổ tối thiểu.
- Target cho RTT kế tiếp lấy tại `t + srtt`, chặn trong `[cwnd, 1.5*cwnd]`. Không lấy RTO thay SRTT; khi chưa có SRTT, lookahead bằng 0.
- `W_est` bắt đầu ở cửa sổ đầu giai đoạn; alpha ban đầu `9/17`, chuyển thành 1 khi ước lượng đạt `cwnd_prior`. Trong vùng Reno-friendly dùng ước lượng này, ngoài vùng đó tăng theo target. ACK gộp được xử lý từng packet mới để nhất quán với ACK lẻ.
- Khi loss, ghi nhớ cửa sổ trước giảm. Fast convergence mặc định bật cho môi trường nhiều kết nối của project; `CubicController(fast_convergence=False)` có thể dùng cho thử nghiệm một flow cô lập.
- Phản ứng loss của project: `ssthresh = max(0.7 * min(cwnd_trước, flight_trước_ACK), 2)`, `cwnd = ssthresh`. Dùng flight trước ACK để không coi các packet vừa được xác nhận là chưa từng ở trên mạng; chặn bởi cửa sổ cũ để flight cũ còn lớn không tăng cửa sổ khi escalation.
- Timeout đặt `cwnd=1`, cập nhật ngưỡng như trên. Sau slow start, lần avoidance đầu dùng K=0 và W_max bằng cửa sổ bắt đầu giai đoạn mới.

`Connection` đánh dấu các packet đang bay khi inflight đạt ngân sách cửa sổ. ACK cho flight đó vẫn được tăng cửa sổ khi số packet còn lại giảm; nếu chỉ nhìn inflight sau mỗi ACK thì một flight đầy sẽ mất quyền tăng quá sớm. Flight chưa từng đạt ngân sách do ứng dụng gửi ít hoặc credit thấp không làm CUBIC tăng cửa sổ.

Khi không còn packet thuộc flight đã dùng đủ cửa sổ, controller tạm dừng đồng hồ congestion avoidance. Khi sử dụng lại đủ cửa sổ, mốc epoch được dịch theo khoảng dừng. Vì vậy thời gian chờ producer/consume/credit không tạo target tăng đột ngột. Đây là cơ chế bảo thủ của project, không phải toàn bộ cơ chế xác thực cửa sổ của TCP.

## 4. Tích hợp A3/A5

Recovery epoch của A3 khác với epoch thời gian CUBIC. Connection tiếp tục sở hữu snapshot highest-sent theo từng stream, distinct-SACK, retry/backoff và ngân sách retransmit:

1. Kiểm ACK toàn bộ trước thay đổi state; lấy flight size trước xóa các packet được ACK.
2. ACK trùng/sai không tăng cửa sổ. Các packet mới ACK của flight đủ điều kiện mới được tính cho CUBIC.
3. Fast loss giảm một lần khi vào recovery. Có thể escalation sang timeout đúng một lần trong cùng recovery; không giảm theo từng packet mất.
4. ACK sửa recovery, kể cả ACK kết thúc snapshot, không tăng cửa sổ. Congestion avoidance bắt đầu lại trên ACK đủ điều kiện tiếp theo.
5. DATA mới vẫn cần đồng thời cwnd, sequence window, stream credit và connection credit. Scheduler A5 vẫn áp dụng ngân sách chat/file.
6. Retransmit không tiêu credit lần hai và giữ slot A3 riêng: vẫn gửi phục hồi khi inflight >= cwnd đã giảm, tối đa một packet mỗi 20ms.
7. Cancel dọn flight và cập nhật trạng thái limited. Close vẫn đánh thức waiter và dọn buffer theo A5.

Startup log ở INFO ghi `algorithm` và cửa sổ khởi tạo. DEBUG log ACK/loss/timeout ghi monotonic time, conn_id, algorithm, cwnd, inflight, số packet mới ACK và trạng thái recovery. `TransportStats.algorithm` lấy trực tiếp từ controller.

## 5. Kiểm thử và bằng chứng

Chạy nhanh (bỏ riêng stress memory A5):

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_congestion.py tests/test_recovery.py tests/test_flow_control.py -q -p no:cacheprovider -k "not large_transfer"
```

Kết quả: **91 passed, 1 deselected**. Log: [A4-quick-tests.txt](evidence/A4-quick-tests.txt).

Môi trường ghi bằng chứng: Windows 11 build 26200, Python 3.14.4, pytest 9.2.0, cryptography 50.0.2. Không thêm dependency mới cho A4.

Test A4 kiểm slow start/threshold, các vector số đã tính trước cho Reno/concave/plateau/convex, ảnh hưởng RTT và thời gian, target cap, fast convergence bật/tắt, signed K, timeout restart, ACK gộp/trùng, idle, credit thấp, pre-ACK flight, cửa sổ chung nhiều stream, controller độc lập qua handshake UDP thật. Các test recovery quan trọng của A3 chạy cả hai thuật toán, gồm loss nhiều stream, snapshot với DATA mới, batch timeout, escalation và loss DATA/ACK/tail/retransmit.

Lệnh toàn bộ:

```powershell
.\.venv\Scripts\python.exe -m pytest -q -rs -s -p no:cacheprovider
```

Kết quả: **154 passed, 1 failed, 4 skipped trong 102,01s**. Log: [A4-full-suite.txt](evidence/A4-full-suite.txt). Baseline A5: 114 passed, 1 failed, 4 skipped; lần chạy A4 tiếp tục đúng lỗi cũ ở `tests/test_auth_routing.py:60`, `test_login_broadcast_private_end_to_end` (assert thiếu broadcast sau điều kiện chờ số lượng event). Không có test khác thất bại. Hai test skip A7 và hai skip thuộc B6 vẫn là công việc riêng; A4 không thay chúng bằng test rỗng hoặc đánh dấu hoàn thành.

Stress A5 chạy lại dưới mặc định CUBIC: tracemalloc peak 743368 byte với 10 MiB và 740384 byte với 100 MiB; sender/receiver payload peak cùng 262144 byte. Đây là bộ nhớ Python theo tracemalloc trong test transport, không phải RSS của toàn ứng dụng. Chat p95 với file blocked 0,004517s, dưới ngưỡng test 0,25s; không suy rộng thành số đo LAN.

## 6. So sánh cùng mô hình mạng

```powershell
.\.venv\Scripts\python.exe -m tests.congestion_scenario --output docs/evidence/A4-comparison.json --trace docs/evidence/A4-cwnd.csv
```

Mô phỏng dùng packet/crypto/ACK/credit/recovery thật và đồng hồ ảo, không mở socket. Payload 240 KiB (245760 byte), message 8192 byte, delay cố định 25ms mỗi chiều, tick 20ms, seed 1 riêng mỗi chiều. Receiver legacy tự consume sau callback. Không có mô hình bandwidth, bottleneck queue hay cạnh tranh nhiều flow; goodput chỉ mô tả mô phỏng này.

| Kịch bản | AIMD: thời gian / retransmit | CUBIC: thời gian / retransmit |
|---|---:|---:|
| Không loss | 0,325s / 0 | 0,325s / 0 |
| Bỏ lần DATA seq=1 đầu tiên | 0,965s / 1 | 0,950s / 1 |
| Loss ngẫu nhiên 10% mỗi chiều | 5,200s / 33 | 8,970s / 39 |

Cả sáu ca xác nhận payload đúng thứ tự/đúng một lần và SHA-256 giống nhau; sender ACK đủ byte, pending/unacked rỗng. Trace gồm thời gian, cwnd, inflight, ACK bytes, retransmit và số lần giảm cửa sổ. Xem [JSON](evidence/A4-comparison.json), [CSV](evidence/A4-cwnd.csv).

CUBIC chậm hơn AIMD trong ca seed 1 loss 10% này. Cửa sổ nhỏ, recovery và nhiều lần timeout ảnh hưởng kết quả; không suy rộng thành đánh giá hiệu năng CUBIC trên Internet hoặc bảo đảm CUBIC luôn nhanh hơn. Hai thuật toán dùng cùng tham số mô hình và seed, nhưng thứ tự gửi/control khác nhau có thể khiến loss rơi vào packet khác nhau. Kiểm chứng mô hình congestion công bằng/băng thông và thử LAN vẫn cần ở A7/J3.

## 7. Ranh giới bàn giao

A4 cung cấp controller, cấu hình mặc định, stats/log và bằng chứng trên. Các API send/consume/receipt/reset và giới hạn A5 giữ nguyên. A6 tiếp tục audit crypto/endpoint resilience; A7 hoàn thành HoL/disconnect và bằng chứng transport; B tiếp tục tích hợp ứng dụng/file/progress/reconnect. Log bằng chứng A5 là lịch sử AIMD, không dùng thay số liệu CUBIC mới.
