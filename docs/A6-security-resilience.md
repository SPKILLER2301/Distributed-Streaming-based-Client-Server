# A6 — Crypto và Endpoint resilience

Ngày: 11/11/2026. Baseline revision: `8d8cef7` (đã có A4/A5 và fix test broadcast). A6 giữ wire v2, header 23 byte và API send/consume/receipt/reset của A5. Không thêm dependency.

## 1. Lỗi được sửa và hành vi mới

| Phạm vi | Trước A6 | Sau A6 |
|---|---|---|
| HELLO từ cùng địa chỉ | Đóng phiên cũ trước kiểm key mới | Kiểm field/key và cấp ID mới trước thay phiên |
| Số connection | Không có cap tổng | Mặc định 64/Endpoint, cấu hình được |
| HELLO trùng | Có reply cache | Giữ hành vi, vẫn trả cache khi đạt cap; không refresh liveness |
| Reply | Có kiểm địa chỉ pending | Thêm validation header/key, không ghi đè ID đang hoạt động |
| Header DATA/control | Một số field không dùng chưa kiểm | DATA ack/sack=0, seq dành cuối bị loại; PING/PONG/CLOSE sid=0 và control seq=0 |
| Counter cạn | Có thể raise giữa close/flow tick | Đóng với reason rõ ràng, fail waiter, không wrap hoặc tạo lại probe sau cleanup |
| UDP đóng khi còn write | Không chờ socket callback | Close có deadline, abort khi cần, vẫn chờ callback socket đóng |
| Lifecycle | Start lặp có thể tạo thêm socket/timer | Start/stop serialize, start lặp báo lỗi, stop lặp an toàn, restart sau stop |
| Socket mất | Thiếu cleanup trên connection_lost | Fail connection/handshake, dọn cache, cancel timer/consumer |

`tests/test_auth_routing.py::test_file_transfer_end_to_end` đã phát hiện một regression trong lượt đầu: chờ callback socket đóng quá hạn khi dừng client sau truyền file. Đọc implementation Proactor của Python 3.14 cho thấy close có thể còn write pending. A6 xử lý bằng close bình thường tối đa 0,05s, sau đó abort pending I/O và chờ callback thêm tối đa 0,5s. Nếu callback vẫn không tới, stop raise TimeoutError; không coi tài nguyên đã dọn. CLOSE cuối là best effort, receipt chưa ACK đã fail khi connection đóng.

Lượt đầu được giữ ở [A6-first-regression.txt](evidence/A6-first-regression.txt) để đối chiếu với kết quả cuối. Các lượt chẩn đoán đã dừng thủ công khi gặp treo trước khi có deadline không được tính là kết quả đạt.

## 2. Cấu hình và ranh giới trách nhiệm

```python
from transport.endpoint import Endpoint

server = Endpoint(is_server=True)  # max_connections=64, congestion_algorithm="cubic"
small_server = Endpoint(is_server=True, max_connections=20)
```

`max_connections` là số nguyên 1..65535, tính tất cả transport session trước/sau auth. Mỗi connection có tối đa stream/buffer theo FlowLimits A5. Khi đầy, bỏ HELLO tạo mới, không đuổi client khác; caller hết deadline handshake sẽ báo không kết nối được. HELLO trùng được trả cached reply; phiên cùng địa chỉ có HELLO mới hợp lệ có thể được thay thế ngay cả khi đầy. ID ngẫu nhiên tránh đụng các connection hiện có, tối đa 16 lần thử; nếu không cấp được thì giữ phiên cũ.

Auth-specific deadline, cap theo trạng thái đăng nhập, password storage, file/app cleanup, reconnect và UI thuộc B. Endpoint không biết username/auth state. Peer im lặng được dọn theo PEER_TIMEOUT hiện có (10s), giải phóng slot và handshake cache.

Địa chỉ vẫn là IPv4 như implementation trước; `connect()` chỉ cho client Endpoint đã start và chỉ có một pending handshake. `local_addr` khi dừng báo RuntimeError. Start/stop dùng lock để thao tác lifecycle không chồng nhau. Stop từ async receive handler được hỗ trợ, kể cả handler gọi stop trong finally khi bị stop khác cancel.

Reason bổ sung: `peer_restarted`, `socket_closed`, `transport_error`. Các reason trước đó giữ nguyên, gồm `endpoint_stopped` và `control counter exhausted`. B dùng reason để hiển thị, không suy luận từ exception nội bộ của socket.

## 3. Validation packet và handshake

- HELLO/HELLO_REPLY: public key đúng 32 byte; flags, stream_id, seq, ack, sack đều 0. HELLO conn_id=0; reply conn_id!=0. Khóa low-order/không hợp lệ bị loại qua CryptoError.
- Kiểm key mới trước khi thay connection cũ. Reply chỉ được nhận từ remote pending, không ghi đè conn_id đã có. Reply tới muộn sau cancel/stop bị bỏ.
- Decode tiếp tục kiểm version/type/length/MAX_DATAGRAM. Connection còn kiểm conn_id/version trước xử lý (kể cả gọi trực tiếp trong test).
- DATA: flags chỉ FLAG_EOM, ack/sack=0, seq<=2^32-2, payload wire từ 16 đến 1177 byte. Tag/AAD phải đúng trước reassembly/delivery. Sender giữ slot seq cuối cho ACK và không wrap.
- Control: length phải khớp body+counter+tag; field không dùng kiểm trước decrypt. PING/PONG/CLOSE dùng sid=0; CLOSE reason trong 0..5. Header/tag/counter bảo vệ tính toàn vẹn, bitmap replay 256 giữ nguyên A5.
- Body có ngữ nghĩa sai được xử lý sau tag: counter của control có tag hợp lệ nhưng body sai có thể đã được ghi nhận; nó không tự chứng minh liveness hoặc ACK progress. Tag sai không được ghi counter vào replay window.

## 4. Khóa, nonce và retransmit

Giữ X25519 + HKDF-SHA256 + AES-256-GCM. Mỗi handshake tạo key pair mới, mỗi chiều có khóa riêng. Nonce có thể trùng giá trị giữa hai chiều vì khóa khác nhau; điều bắt buộc là không tái dùng **cặp khóa–nonce** để tạo bản mã mới.

- DATA nonce: `00 || sid:u16 || seq:u32 || 5 zero bytes`.
- Control nonce: `01 || counter:u64 || 3 zero bytes`.
- SessionCipher kiểm kiểu/phạm vi của sid, seq, counter trước pack; không nhận bool làm số.
- Control dùng counter mới toàn connection cho mỗi lần phát, kể cả retry. Khi cạn thì đóng, không wrap.
- DATA chỉ encrypt ở lần phát đầu. Retransmit dùng cached datagram bất biến; ACK/cancel/close giải phóng ownership cache. Không tái sử dụng stream đã hủy dưới cùng khóa.
- Duplicate DATA hợp lệ không giao app lại nhưng ACK với control counter mới. ACK replay không tăng receipt/cwnd.

Cơ sở thư viện: [AESGCM/AEAD 50.0.2](https://cryptography.io/en/50.0.2/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM) yêu cầu nonce không tái dùng với cùng khóa và xác thực AAD; [X25519](https://cryptography.io/en/latest/hazmat/primitives/asymmetric/x25519/) hướng dẫn lấy khóa qua KDF và tạo private key mới cho handshake tiếp theo. A6 kiểm quy tắc ở transport qua test, không tự viết primitive mật mã.

## 5. Socket errors và quan sát

`sendto()` OSError và `error_received()` (bao gồm mô phỏng WinError 10054) tăng counter và log có giới hạn. Không đóng toàn bộ phiên chỉ vì một ICMP: callback UDP không xác định đáng tin cậy connection nào gây lỗi. Timeout của từng peer lo phát hiện mất kết nối. Socket thực sự `connection_lost` thì kết thúc các phiên trên socket đó và đánh thức handshake pending.

Endpoint giữ counters tổng `invalid_packets`, `invalid_handshakes`, `resource_rejections`, `socket_errors`, `processing_errors`; không có map theo địa chỉ tăng vô hạn. Lỗi socket/process log tối đa một lần/giây/Endpoint. Malformed packet thông thường bị loại im lặng và đếm. Timer gặp lỗi ngoài dự kiến ở một connection thì log và đóng riêng connection với `transport_error`.

Lỗi UDP được inject để tái lập độc lập với chính sách ICMP của Windows; test UDP thật chứng minh sau packet rác/HELLO hỏng/server đầy, client đã kết nối vẫn gửi chat và nhận ACK. Đây không phải kiểm thử mọi driver/OS hoặc LAN thực tế.

## 6. Kiểm thử và kết quả

Môi trường: Windows 11 build 26200, Python 3.14.4, pytest 9.2.0, cryptography 50.0.2.

Baseline mới sau sửa test broadcast: **155 passed, 4 skipped trong 43,45s**, [A6-baseline.txt](evidence/A6-baseline.txt).

Chạy nhóm crypto/resilience/packet:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_crypto.py tests/test_endpoint_resilience.py tests/test_packet.py -q -p no:cacheprovider
```

**101 passed**, [A6-quick-tests.txt](evidence/A6-quick-tests.txt). Gồm 5 ca packet cũ và 96 ca mới (tính parameterization). Các test A3/A5 còn kiểm receipt/reset/consume waiter, stream cap, retry exhaustion và immutable ciphertext.

```powershell
.\.venv\Scripts\python.exe -m pytest -q -rs -s -p no:cacheprovider
```

Kết quả cuối: **251 passed, 4 skipped trong 80,08s**, [A6-full-suite.txt](evidence/A6-full-suite.txt). Hai skip A7 về HoL/disconnect và hai skip B6 về app/file còn là công việc riêng. Không bỏ skip khi chưa có thân test/assertion tương ứng.

Regression A5 chạy lại: payload 10/100 MiB đều đủ hash/byte; payload peak sender/receiver cùng 262144 byte. Tracemalloc peak lần lượt 744160/740512 byte; không phải RSS toàn ứng dụng. Chat p95 khi file blocked 0,0014761s, dưới ngưỡng test 0,25s, là loopback cùng process. Loss 10% seed 1 nhận đủ 200000 byte, 24 retransmit trong lượt này; lịch asyncio có thể làm số retransmit khác giữa các lượt dù cùng seed.

## 7. Giới hạn bảo mật và việc tiếp theo

Handshake hiện chưa xác thực danh tính server/client và chưa có key confirmation; không chống MITM chủ động hoặc giả mạo địa chỉ gửi HELLO hợp lệ. Validation chỉ ngăn HELLO hỏng làm mất phiên. HELLO mới hợp lệ từ cùng địa chỉ vẫn có thể thay phiên theo contract hiện tại. Chưa có cookie/anti-amplification hoặc rate limit crypto cho flood nhiều địa chỉ; cap connection giới hạn state, không bảo đảm chống DoS CPU/băng thông.

API SessionCipher là primitive nội bộ, không giữ nonce ledger toàn phiên; tính duy nhất do Connection/SendStream đảm bảo qua sequence, tombstone và cache. Test riêng có gọi primitive trực tiếp để tạo packet sai; ứng dụng chỉ dùng Endpoint. Mã hóa từng chặng qua server relay, không phải E2E giữa người dùng.

A7 tiếp tục test HoL với drop đích danh, peer disconnect và bằng chứng transport/LAN. B tiếp tục auth, bootstrap production integration, file state/progress, reconnect và load; A8 tổng hợp báo cáo dựa trên số liệu thật. A6 không xác nhận các task đó đã hoàn thành.

Cập nhật sau A6: [A7 — kiểm thử transport](A7-transport-tests.md) đã hiện thực HoL/disconnect, receiver chậm và cleanup; kết quả A6 phía trên được giữ như mốc lịch sử. LAN hai máy vẫn thuộc J3, chưa được xác nhận bằng test loopback A7.
