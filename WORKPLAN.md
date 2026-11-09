# WORKPLAN — Phân công A/B hoàn thiện Assignment 1

> Cập nhật: **09/11/2026**. Căn cứ: [README](README.md), [pipeline](pipeline.md) và [đề bài DOCX](<Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application.docx>).
> DOCX quy định yêu cầu; pipeline hướng dẫn triển khai; WORKPLAN quy định người làm, phụ thuộc và nghiệm thu. Đây là kế hoạch, chưa xác nhận task nào hoàn thành.

## 1. Cách chia lại và phạm vi

- **A — Transport và giao thức:** TCP bootstrap, UDP reliability, flow control, scheduler, CUBIC, mã hóa packet và test transport.
- **B — Application và UI:** auth/user, routing, toàn bộ file transfer/relay, reconnect, HTTP UI, test app/load và đóng gói.
- **Cả hai:** baseline, chốt API, review, tích hợp, LAN, báo cáo và kiểm bản nộp. Giờ chung được tính cho từng người.

Giữ crypto và CUBIC ở A vì chúng liên quan trực tiếp tới ACK/recovery/nonce trong Connection. Chuyển `client/file_transfer.py` sang B để cùng một người quản lý producer, relay, receiver và progress UI. B phụ trách lưu mật khẩu và phiên ứng dụng; A phụ trách bảo mật trên đường truyền.

Các quyết định bám đề:

1. **TCP bootstrap là công việc mặc định** theo Step 2. A làm module, B tích hợp vào đường khởi động thật. HTTP browser–daemon không thay thế kết nối client–server này.
2. Chat/file dùng **UDP tin cậy tự xây**, server relay. TCP bootstrap + UDP là lựa chọn dung hòa các đoạn TCP/UDP chưa thống nhất trong DOCX; giải thích lựa chọn trong báo cáo.
3. **CUBIC nằm trong phạm vi cần làm** theo phần Techniques (BBR/CUBIC), không phải việc cắt đầu tiên. AIMD hiện tại là baseline.
4. Giữ HTTP server tự viết trên socket/`asyncio`, JavaScript phía client và SSE; không Flask/Django/WebSocket. Không thêm P2P hoặc tự viết TCP stack theo tài liệu tham khảo.
5. Chọn 20 client để kiểm mức 10–20 client của đề. Loss 10%, file 10/100 MB và ngưỡng RAM/latency là tiêu chí thử nghiệm nội bộ, không phải điểm rubric riêng.
6. Salt/KDF cho mật khẩu và xác thực control packet là hardening. Challenge-response, resume, E2E, ETA, ảnh Wireshark không phải yêu cầu nguyên văn bắt buộc.

## 2. Chủ sở hữu file

| File / phạm vi | Owner | Ranh giới |
|---|---|---|
| `common/packet.py`, `common/constants.py` | A | Header, version, giới hạn; B đề xuất thay đổi hằng số qua A |
| `transport/endpoint.py`, `connection.py`, `stream.py` | A | Reliability, credit, scheduler, API, stats, đóng/hủy |
| `transport/congestion.py`, `crypto.py` | A | CUBIC, RTT/RTO, khóa/nonce và control packet |
| `transport/bootstrap.py` (dự kiến) | A | TCP framing, ACK, timeout, start/stop/connect |
| `common/messages.py` | B | Schema AUTH/chat/file/progress/error |
| `server/*`, `client/client.py`, `client/main.py` | B | Tích hợp bootstrap, auth/registry/router/reconnect |
| `client/file_transfer.py`, `client/web_ui/*` | B | Chunk/hash, file state, upload/progress, HTTP/SSE/UI |
| `tests/test_packet.py`, `test_reliability.py`, `lossy_socket.py` | A | Packet, simulator, reliability, HoL, timeout |
| Test bootstrap/congestion/crypto/flow mới | A | Tạo file riêng khi cần |
| `tests/test_file_transfer.py`, `test_auth_routing.py`, `load_test.py` | B | File cả hai chặng, app, cleanup, 20 client; A cung cấp hook loss |
| `docs/protocol.md` | A chủ trì | B viết mục message/file state; ghép tuần tự |
| `docs/diagrams/` | A: kiến trúc/state/handshake/loss; B: chat/file | Tách file theo sơ đồ |
| README, WORKPLAN, pipeline, PDF | B ghép, A review | Mỗi người viết nội dung phần mình |

Owner chịu trách nhiệm kết quả và ghép thay đổi; người kia review. Thư mục hiện tại chưa có `.git`: chỉ áp dụng nhánh/PR sau khi có repository, không giả định đã có quy trình Git. TODO(A)/TODO(B) trong code có thể còn owner cũ; cập nhật khi nhận task theo bảng này.

## 3. Công việc cụ thể

Ước lượng là **giờ công dự kiến**, gồm test riêng của task. Chưa coi số “22 pass, 4 skip” cũ là baseline đã xác nhận; cần chạy lại và lưu log. Mỗi task phải có bằng chứng trước khi đánh dấu xong.

### 3.1 A — 44 giờ riêng

| ID | Việc / mục pipeline | Giờ | Step | Điều kiện xong |
|---|---|---:|---|---|
| A1 | Audit transport; spec/state/API trước sửa (§5–6) | 4 | 1 | Ghi có/thiếu/lỗi; chốt API Endpoint, đơn vị byte/packet, giới hạn, nonce/control; tách API hiện có và dự kiến |
| A2 | TCP bootstrap (§7) | 3 | 2 | Module và test ACK ứng dụng, response chia nhiều lần đọc, version sai, timeout, request quá lớn; bàn giao cho B1 |
| A3 | Reliability (§8) | 7 | 6, 7 | Fast retransmit theo SACK khác nhau, backoff, recovery epoch, ngân sách retransmit; test mất DATA/ACK/gói cuối, duplicate, hết retry; giao đúng thứ tự đúng một lần |
| A4 | CUBIC (§9) | 5 | Techniques, 6 | Ghi nguồn đặc tả; test thời gian giả cho slow start/ACK/loss/timeout/cwnd; không giảm lặp trong một recovery; bật trong demo |
| A5 | Flow control + scheduler (§10) | 8 | 5, 6 | Credit stream/connection, queue/fragments có trần; update mất/trùng vẫn tiến triển; control/chat có ngân sách, file không bị bỏ đói; bàn giao send/ACK/consume/cancel/stats cho B3 |
| A6 | Crypto + endpoint resilience (§12–13) | 6 | 7, 8 | Mã hóa hai chiều, tamper bị loại, DATA retransmit bất biến, không dùng nonce cho DATA/AAD khác; xác thực control có counter/miền nonce riêng theo spec; giới hạn stream/seq/connection, socket lỗi không sập server, close đánh thức waiter |
| A7 | Test transport và bằng chứng (§15) | 6 | 4–8, 10 | Hoàn thiện 2 test skip thuộc A; HoL bằng drop đích danh, receiver chậm, cleanup; log seed/cwnd/retransmit/queue; hỗ trợ simulator cho B6 |
| A8 | Tài liệu và phần báo cáo (§16) | 5 | 1, 10 | Protocol khớp encoder, ví dụ packet; kiến trúc/state/handshake/loss; viết transport/security và đóng góp Challenges từ lỗi thực tế |
| | **Tổng A riêng** | **44** | | |

A1 thiết kế trước triển khai; A8 cập nhật bản cuối. Chốt nonce/control cùng A1 để A3/A5 không phải đổi format muộn.

### 3.2 B — 44 giờ riêng

| ID | Việc / mục pipeline | Giờ | Step | Điều kiện xong |
|---|---|---:|---|---|
| B1 | Audit app, schema/state và tích hợp TCP (§5–7) | 4 | 1, 2 | Chốt message/file state trước sửa; gọi A2 trong lifecycle server/client thật, CLI/cổng/start-stop đúng; TCP ACK → UDP → login |
| B2 | Auth, registry, routing (§13) | 4 | 3–5 | Login đúng/sai/trùng/lặp; user–IP–port được quản lý và dọn; broadcast/DM đúng người; PBKDF2/scrypt có salt và tài khoản demo tương ứng |
| B3 | File transfer, relay, backpressure (§11) | 10 | 5–7 | OFFER/ACCEPT/BEGIN/DONE đúng sender/receiver/state; file 0/1/8192/8193 byte, Unicode, size/path/hash; producer/relay dùng A5; progress phân biệt enqueue/ACK chặng đầu/DONE; FILE_ERROR và cleanup khi ngắt |
| B4 | Lifecycle và reconnect (§13) | 5 | 4, 7 | Một task reconnect backoff+jitter chạy lại TCP/UDP/auth; stop/logout hủy retry; server offline vẫn mở UI; không giữ phiên/file/task mồ côi, không ngầm resume file |
| B5 | HTTP UI và upload streaming (§11,14) | 7 | 9 | Chat/user/progress/network status; upload theo chunk có trần/timeout; giữ Nhận/Từ chối, kiểm XSS; SSE cleanup, phân biệt hai kết nối; chọn một tab/daemon hoặc fan-out có giới hạn |
| B6 | Test app/file/load (§15) | 8 | 3–10 | Hoàn thiện 2 test skip thuộc B; file qua loss cả hai chặng đúng hash; crash dọn user/file; 20 client có barrier, expected/received/duplicate, timeout và exit code lỗi; log RAM/progress |
| B7 | README, PDF và gói nộp (§16) | 6 | 10; Submission | README từng bước; viết app/UI, chat/file sequence, evaluation; ghép phần A và Challenges khoảng 1 trang; xuất PDF, chuẩn bị ZIP/private repo đủ source/dependencies |
| | **Tổng B riêng** | **44** | | |

Không viết lại chunk/hash, nút Từ chối và `textContent` đã có; kiểm và hoàn thiện hành vi thiếu. Hủy đang truyền nếu bổ sung phải có message/state riêng. Cleanup khi lỗi vẫn là việc cần làm.

### 3.3 Việc chung — 8 giờ/người

| ID | Hoạt động | Giờ mỗi người | Kết quả |
|---|---|---:|---|
| J1 | Baseline và chốt hợp đồng API | 2 | Cùng môi trường/lệnh, log pass/fail/skip thật, thống nhất interface; không tính lặp audit riêng A1/B1 |
| J2 | Review và tích hợp đầu cuối | 2 | Bootstrap → auth → chat/file; kiểm loss/credit/DONE và lỗi |
| J3 | LAN thật hai máy | 2 | Cả hai tham gia; kiểm TCP+UDP, chat/file, kill client, lưu cấu hình/hash/log |
| J4 | Kiểm bản nộp trên môi trường sạch và đọc chéo | 2 | Làm theo README, mở PDF/gói nộp, giải thích được phần của nhau |
| | **Tổng chung mỗi người** | **8** | |

**Tổng: A = 52 giờ, B = 52 giờ, nhóm = 104 giờ công.** Dự phòng thêm **4–6 giờ/người** cho lỗi tích hợp, không dùng để thêm tính năng. J3 kéo dài 2 giờ nhưng tính 4 giờ công nhóm.

## 4. Hợp đồng bàn giao và phụ thuộc

Ghi hợp đồng trong `docs/protocol.md` tại J1/A1/B1, trước khi sửa protocol:

| Giao diện | A cung cấp | B sử dụng / kiểm tra |
|---|---|---|
| Bootstrap | Framing, version, cổng UDP, timeout, start/stop/connect | Tích hợp startup/reconnect thật |
| Send có chờ | Chữ ký async, queue tính theo byte, lỗi khi close/cancel | Producer/relay await; không dùng sleep thay backpressure |
| ACK progress | Bytes payload mới ACK, ánh xạ stream, không đếm retransmit | Nhãn “đã tới server”; chỉ xong sau DONE hợp lệ từ receiver |
| Receive/consume | Callback sync hay consumer async, quyền sở hữu buffer và trả credit | Không block callback; trả credit sau tiêu thụ/chuyển vào buffer giới hạn |
| Close/cancel | Hủy stream, giải phóng pending, đánh thức waiter | Hủy task, xóa file dở, cập nhật transfer/UI đúng phạm vi |
| Stats/error | RTT, retransmit, trạng thái và lý do lỗi ổn định | Đưa vào API/event UI, không để JS đọc nội bộ transport |

B đang gọi **Endpoint**, không chỉ Connection/Stream. `send()` hiện chỉ xác nhận enqueue. API có chờ/ACK/consume/cancel là phần **cần thêm**.

Phụ thuộc chính: J1 → A1/B1 → A2 và tích hợp B1 → A3/A4/A5/A6 → tích hợp B3/B4 → A7/B6/J2 → J3 → A8/B7/J4. Tài liệu viết xuyên suốt.

B làm B2, file state/file rỗng của B3, upload/UI B5 và khung lifecycle B4 trong lúc A làm transport. Có thể dùng stub theo hợp đồng khi phát triển; nghiệm thu phải dùng transport thật. A5 cần giao API tối thiểu trước khi B3 hoàn tất backpressure. Thay API phải báo người kia, cập nhật spec và test liên quan cùng lần tích hợp.

## 5. Lịch dự kiến từ 09/11/2026

DOCX ghi “Sunday, November 17, 2026, at 11:59 PM”, nhưng 17/11/2026 là **thứ Ba**. Dùng **17/11/2026, 23:59** làm hạn dự kiến và đối chiếu thông báo chính thức; mục tiêu nội bộ **trước 12:00 ngày 17/11**. Không coi mốc 07/11 cũ là đã xong.

| Mốc | A | B | Điều kiện chuyển mốc |
|---|---|---|---|
| 09/11 | A1, J1, chốt nonce/control/API | Audit/schema B1, J1; bắt đầu B2 | Baseline thật, spec/API dùng được |
| 10/11 | A2, bắt đầu A3 | Tích hợp B1, B2; file state B3 | TCP ACK thật rồi login UDP |
| 11–12/11 | A3/A4/A5/A6; API A5 tối thiểu ngày 11 | B3/B4/B5 theo API | Reliability/CUBIC/credit nối được relay; progress đúng nghĩa |
| 13–14/11 | Xong phần lõi, A7, viết A8 | Xong B3–B5, B6, viết B7 | J2 đạt; mỗi Step có đường chạy và bằng chứng |
| 15/11 | J3, sửa lỗi, hoàn thiện A8 | J3, B6/B7, sửa lỗi | LAN có log; đóng băng tính năng cuối ngày nếu đạt |
| 16/11 | J4, review PDF/README, sửa lỗi chặn nộp | Ghép PDF/gói nộp B7, J4 | Bản sạch chạy được và đủ tài liệu |
| 17/11 trước 12:00 | Cùng kiểm bản nộp | Nộp và xác nhận file/link mở được | Hoàn tất theo kênh giảng viên quy định |

52 giờ/người trong 8 ngày 09–16/11 tương đương **6,5 giờ/ngày**, chưa tính dự phòng. Cân bằng giờ không có nghĩa độ khó ngang nhau; cập nhật dự toán sau baseline. Nếu chỉ rảnh 4 giờ/ngày thì có khoảng 32 giờ/người, thiếu khoảng 20 giờ: cần điều chỉnh thời lượng/phạm vi cải thiện hoặc trao đổi hạn, không âm thầm bỏ yêu cầu đề.

## 6. Nghiệm thu theo rubric

| Step (1 điểm/Step) | Chủ trì | Bằng chứng |
|---|---|---|
| 1 — Protocol | A; B message | Header/message/state trước code mới; bản cuối khớp encoder |
| 2 — TCP handshake | A; B tích hợp | Client daemon TCP connect tới server, nhận ACK ứng dụng; test lỗi/timeout |
| 3 — Auth/state | B | Đúng/sai/trùng/lặp; bảng user/IP/port và cleanup |
| 4 — Concurrency | B; A endpoint | 20 client login/chat/connect/disconnect có assertion/số liệu |
| 5 — Routing/streaming | B; A HoL | DM không tới người thứ ba; broadcast đúng tập; chat qua khi file thiếu gói |
| 6 — UDP file | B; A reliability | File qua relay/loss đúng size/hash, receiver chậm có trần RAM |
| 7 — Resilience | B app; A socket | Kill sender/receiver, timeout, restart, cleanup; server còn phục vụ |
| 8 — Encryption | A; B tích hợp | Encryption/decryption/tamper/replay; nonce/retransmit đúng; mô tả mã hóa từng chặng và hạn chế handshake |
| 9 — HTTP UI | B | Hai browser hiển thị chat/user/progress/network status và lỗi rõ ràng |
| 10 — Test/quality | A+B | Suite kết quả thật, simulation/LAN nhiều terminal, code có chú thích, README tái hiện được |

CUBIC, sequence/sliding window, async streaming/tách tầng, 10–20 client còn được kiểm theo Techniques/Design, không gán thành điểm thứ 11.

Phân lại **bốn test skip**:

| Test | Owner | Phối hợp |
|---|---|---|
| `test_no_head_of_line_blocking` | A (A7) | B cung cấp luồng chat/file tích hợp |
| `test_peer_disconnect_detected` | A (A7) | Phân biệt timeout transport và cleanup app |
| `test_file_over_lossy_network` | B (B6) | A cung cấp simulator/hook; kiểm cả hai chặng relay |
| `test_client_crash_is_cleaned_up` | B (B6) | A cung cấp sự kiện disconnect đúng hạn |

Viết thân test/assertion/timeout/cleanup thật rồi mới bỏ skip. Lưu bằng chứng trong `docs/evidence/`: ngày, OS/Python, revision nếu có, lệnh, size file, seed/loss/delay/jitter/duplicate từng chặng, hash, kết quả và giới hạn thử nghiệm.

## 7. Review, tài liệu và ưu tiên

- Mỗi task ghi file sửa, hành vi trước/sau, lệnh/kết quả test, log và API thay đổi. Chạy test liên quan trước, toàn suite khi tích hợp; người kia review giao tiếp và bằng chứng.
- PDF đủ **System Architecture, Protocol Specification, Sequence Diagram (message và file), Challenges & Solutions khoảng 1 trang**. Evaluation dùng số liệu thật. A8/B7 chia nội dung, B ghép, A kiểm lại.
- ZIP hoặc link GitHub private chứa đủ source client/server/module phụ thuộc, README, dependencies và PDF. J4 kiểm môi trường sạch, font/sơ đồ, đường dẫn và quyền truy cập bản nộp.
- Cắt trước: làm đẹp UI, ETA/biểu đồ phụ, thống kê/chống spam nâng cao, ảnh minh họa bổ trợ, hủy chủ động nếu chưa chốt. Không mở thêm resume/P2P/E2E/challenge-response. Hardening ngoài rubric ước lượng lại sau baseline; nếu hoãn phải ghi hạn chế.
- Giữ yêu cầu đề: TCP Step 2, UDP file tin cậy, login/discovery/routing, async nhiều client, CUBIC đã chọn, payload encryption, HTTP UI, resilience, kiểm thử và hồ sơ nộp. Giữ giới hạn bộ nhớ/cleanup đủ chứng minh hệ thống ổn định; không mở rộng thành QUIC đầy đủ.

**Bắt đầu:** A nhận A1, B nhận audit/schema B1, cả hai làm J1. Chưa có log thì chưa đánh dấu baseline hoặc Step nào hoàn thành.
