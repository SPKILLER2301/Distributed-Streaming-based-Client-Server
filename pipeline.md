# PIPELINE — Hướng dẫn hoàn thiện Assignment 1

> Cập nhật: 08/11/2026. Đây là hướng dẫn thực hiện, đối chiếu trực tiếp với mã nguồn hiện có.
> **Nguồn yêu cầu chính thức duy nhất:** [Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application.docx](<Assignment 1 - Distributed Streaming-based Client-Server Collaboration Application.docx>).
> **`reference/` chỉ để tham khảo.** Không lấy yêu cầu P2P, proxy, cookie, channel hay raw TCP tự dựng trong đó để thay thế DOCX.
> Phân công cũ: [WORKPLAN.md](WORKPLAN.md). Những chỗ còn khác với tài liệu này cần được cập nhật; chưa coi WORKPLAN cũ là kế hoạch đã chốt.

## 0. Cách sử dụng tài liệu này

Đọc mục 1–4 để hiểu mình đang xây gì và đã có gì. Sau đó làm lần lượt mục 5–16. Mỗi lần chỉ hoàn thành một phần nhỏ: đọc code → mô tả thay đổi → sửa → kiểm tra → ghi kết quả.

Ba nhãn trạng thái được dùng xuyên suốt:

- **Đã có trong code:** đã đọc thấy phần triển khai; không đồng nghĩa đã chạy đạt mọi tình huống.
- **Cần làm/hoàn thiện:** phần thiếu hoặc chưa đáp ứng hành vi mong muốn.
- **Cần kiểm chứng:** đã có code/test nhưng cần chạy và lưu kết quả trên môi trường của nhóm.

Không đánh dấu một Step là hoàn thành chỉ vì đã có file tương ứng. Phải có một cách chạy để chứng minh.

**Tình trạng kiểm thử khi rà soát:** lần chạy `python -m pytest -q` trước đó dừng ở collection vì Python đang dùng thiếu `cryptography`. Dependency này đã được khai báo trong `requirements.txt`. Con số “22 pass, 4 skip” là thông tin cũ của README, chưa được xác nhận lại trong lần rà soát. Có đúng bốn test đang gắn `pytest.mark.skip` trong code.

**Ngày nộp:** DOCX ghi “Suggested Deadline: Sunday, November 17, 2026, at 11:59 PM”. Ngày 17/11/2026 thực tế là **thứ Ba**. Dùng ngày 17/11/2026, 23:59 làm mốc dự kiến và đối chiếu thông báo nộp chính thức. Mục tiêu nội bộ: nộp trước 12:00 ngày 17/11 để còn thời gian xử lý lỗi upload.

## 1. Chốt phạm vi theo DOCX

### 1.1 Bảng yêu cầu và cách chứng minh

| Step | Yêu cầu trong rubric | Cách làm của dự án | Bằng chứng cần có |
|---|---|---|---|
| 1 | Thiết kế packet, message, state diagram trước khi code | Ghi đặc tả hiện tại, chốt phần mở rộng trước khi sửa tiếp | `docs/protocol.md`, sơ đồ trạng thái, ví dụ packet |
| 2 | Client kết nối TCP tới server và nhận acknowledgment | Thêm kết nối TCP bootstrap giữa client daemon và server | Test connect TCP → nhận phản hồi ACK của ứng dụng |
| 3 | Login; quản lý user, IP và port | `Auth`, `Registry`, các message AUTH/USER_* | Test đúng/sai/trùng tên và bảng session |
| 4 | Nhiều client kết nối/ngắt đồng thời bằng thread hoặc async I/O | Giữ kiến trúc `asyncio` | Load test 20 client, thống kê thành công/thất bại |
| 5 | Server broadcast/forward message tới đúng người | Giữ server relay cho chat chung và DM | Test ba client: DM không tới người thứ ba |
| 6 | Chia file lớn, truyền và ghép lại qua UDP không hỏng | Stream file riêng, ACK/SACK/retransmit, SHA-256 | File qua mạng mất/đảo/trùng gói vẫn khớp hash |
| 7 | Xử lý timeout, lỗi socket, client thoát mà server không sập | Timeout, cleanup, báo lỗi, khả năng kết nối lại | Kill client, mất mạng, restart server |
| 8 | Mã hóa payload bằng thư viện mật mã, giải mã bên nhận | X25519/HKDF và AES-GCM đang có | Test mã hóa/giải mã, sửa packet bị loại; bản ghi mạng bổ trợ |
| 9 | HTTP UI, JavaScript chỉ phía client; tin nhắn, progress, user list | HTTP server tự viết + HTML/JS + SSE | Demo hai cửa sổ trình duyệt |
| 10 | Code rõ ràng, có chú thích, test mô phỏng mạng cục bộ | Unit/integration test, chạy nhiều terminal, README | Log test và hướng dẫn chạy tái hiện được |

Ngoài bảng 10 Step, DOCX còn nêu: tách network/data/UI; async streaming; loại bỏ HoL giữa luồng; sequence tăng đơn điệu, sliding window, gửi lại phần thiếu; congestion control BBR/CUBIC; phục vụ khoảng 10–20 client.

**Chọn CUBIC làm mục cần thực hiện** để đáp ứng phần Techniques. AIMD hiện tại là baseline. Không tiếp tục ghi “AIMD chắc chắn đủ điểm” hay cắt CUBIC đầu tiên khi chưa có xác nhận thay đổi yêu cầu.

### 1.2 Giải quyết các chỗ DOCX chưa thống nhất

1. **TCP và UDP:** DOCX vừa nhắc TCP cho file/message trong Techniques, vừa yêu cầu custom UDP trong mục tiêu và truyền file UDP ở Step 6. Thiết kế dung hòa của nhóm: TCP bootstrap đáp ứng Step 2; chat/file dùng transport UDP tự xây. Ghi quyết định này vào báo cáo. Đây là cách triển khai được chọn, không phải khẳng định DOCX hết mâu thuẫn.
2. **UI:** mục Design cho phép CLI/GUI nhưng Step 9 nói rõ HTTP + JavaScript. Chọn Web UI để đáp ứng rubric cụ thể.
3. **`asyncio`:** giữ để thực hiện async I/O mà DOCX cho phép. Mẫu tham khảo cũng dùng `asyncio.start_server`, hỗ trợ lựa chọn này nhưng không phải văn bản cấp phép thay DOCX.
4. **`http.server`:** không dùng trong kế hoạch này. Giữ parser/response HTTP tự viết trên `asyncio.start_server`; không cần đổi sang `socket + selectors` chỉ do lo tên gọi “high-level”.
5. **Raw socket:** không tự suy ra phải dùng `SOCK_RAW`, dựng IP/TCP header hoặc chạy sudo như mẫu. Dự án dùng socket UDP/TCP qua Python và tự viết protocol ứng dụng/reliability. Không có task viết lại TCP stack.
6. **P2P:** giữ server relay theo Step 5 DOCX. Không thêm direct P2P vì PDF tham khảo có yêu cầu đó.

**Không dùng:** Flask, Django, WebSocket; thư viện transport làm sẵn thay phần reliability cần tự xây.

**Không phải yêu cầu nguyên văn của đề:** QUIC đầy đủ, packet number mới mỗi lần retransmit, byte offset, các frame tên `MAX_DATA`/`MAX_STREAM_DATA`, challenge-response, resume file, ETA, ảnh Wireshark, test đúng 100 MB hoặc đúng 30% loss. Một số được dùng làm giải pháp hoặc bằng chứng nội bộ, không gọi chúng là các điểm rubric riêng.

## 2. Hiểu hệ thống trước khi sửa

### 2.1 Ba đường kết nối khác nhau

```text
Browser A ── HTTP + SSE, TCP loopback ── Client daemon A
                                           │
                                           ├── TCP bootstrap ── Server
                                           │   [CẦN THÊM]
                                           │
                                           └── UDP tin cậy ───── Server
                                                                  │
                                                            UDP tin cậy
                                                                  │
Browser B ── HTTP + SSE, TCP loopback ── Client daemon B ────────────┘
```

- Browser không trực tiếp gửi UDP. JavaScript gọi HTTP tới daemon trên cùng máy.
- Daemon là chương trình Python chạy `client.main`, chứa transport và logic chat/file.
- SSE là luồng sự kiện trên HTTP (`EventSource`), không phải WebSocket.
- Server giải mã dữ liệu từ A, định tuyến rồi mã hóa lại trên kết nối tới B. Đây là mã hóa từng chặng, **không phải end-to-end encryption giữa A và B**.
- HTTP phục vụ browser cục bộ không tự chứng minh Step 2: Step 2 cần kết nối từ client ứng dụng tới server của hệ thống.

### 2.2 Luồng đăng nhập hiện tại

1. `client/main.py` tạo `CollabClient` và gọi `start()`.
2. `Endpoint` mở UDP; client gửi `HELLO` chứa public key X25519.
3. Server trả `HELLO_REPLY` chứa `conn_id` và public key. Hiện không có trường random riêng trong payload HELLO.
4. Hai bên dùng X25519 + HKDF tạo khóa gửi/nhận riêng từng chiều.
5. Client gửi `AUTH {username, password}` trên stream 0, nằm trong DATA đã mã hóa.
6. Server kiểm mật khẩu, lưu username/conn_id/IP/port, trả `AUTH_OK` hoặc `AUTH_FAIL`.
7. Server gửi sự kiện user online/offline. PING/PONG và timeout duy trì trạng thái kết nối.
8. HTTP UI hiện được mở sau khi `client.start()` thành công. Nếu server chưa bật, UI chưa mở được; cần cải thiện ở bước resilience.

Sau khi thêm bootstrap: thực hiện TCP connect + nhận ACK trước bước UDP; trao đổi khóa UDP vẫn giữ ở HELLO/HELLO_REPLY.

### 2.3 Luồng chat hiện tại

```text
Browser POST /api/chat
→ CollabClient.send_chat()
→ JSON MSG_BROADCAST hoặc MSG_PRIVATE, stream 1
→ Endpoint → Connection → packet DATA mã hóa
→ UDP → server giải mã, ghép message
→ Router → một người nhận hoặc những người online khác
→ client nhận → events queue → SSE → JavaScript hiển thị
```

Chat dùng **stream 1**, không dùng stream control 0. Broadcast hiện không gửi ngược về sender; UI tự hiển thị dòng “Tôi”. Ghi rõ quy ước này khi viết test số lượng tin.

### 2.4 Luồng file hiện tại và điểm cần bổ sung

1. Browser upload file qua HTTP tới daemon A. Hiện server HTTP đọc toàn bộ body vào RAM rồi ghi file tạm.
2. `FileSender` tính SHA-256 theo block và sinh chunk 8192 byte; phần này đã đọc từng chunk, không đọc nguyên file một lần.
3. A gửi `FILE_OFFER` trên stream 0; B chọn nhận hoặc từ chối.
4. Nếu nhận, A mở stream >= 2, gửi `FILE_BEGIN`, rồi từng chunk bytes.
5. Server mở stream khác tới B và relay. Hai chặng có stream ID độc lập.
6. Transport B sắp packet theo `seq`, ghép message theo EOM. `FileReceiver` ghi các chunk đã đúng thứ tự; hiện không ghép file bằng byte offset.
7. B nhận đủ số byte, kiểm SHA-256 và gửi `FILE_DONE {ok}` về A.

**Cần hoàn thiện:** backpressure hai chặng, giới hạn RAM toàn tuyến, file rỗng, cleanup khi lỗi, tiến độ trung thực và xác thực trạng thái OFFER/ACCEPT/BEGIN.

### 2.5 Từ vựng tối thiểu

| Từ | Hiểu đơn giản |
|---|---|
| Message | Một đơn vị ứng dụng: một tin chat, một JSON điều khiển hoặc một chunk file |
| Packet/datagram | Đơn vị gửi UDP; một message có thể bị chia thành nhiều packet |
| Stream | Luồng có thứ tự riêng; mất packet file không bắt chat chờ ghép file |
| `seq` | Số thứ tự DATA trong từng stream, tăng từ 0 |
| ACK | Báo đã nhận; không tự chứng minh người dùng cuối đã lưu file |
| SACK | Chỉ ra một số packet phía sau chỗ thiếu đã tới |
| `cwnd` | Giới hạn lượng dữ liệu đang bay để tránh làm nghẽn mạng |
| Flow control | Giới hạn gửi theo khả năng tiếp nhận của bên nhận |
| Backpressure | Khi phía sau chậm, phía trước phải dừng đưa thêm dữ liệu vào queue |
| RTT/RTO | Thời gian khứ hồi / thời hạn chờ trước khi gửi lại |
| HoL | Dữ liệu đã tới nhưng phải đợi phần thiếu phía trước trong cùng luồng thứ tự |

Các stream vẫn dùng chung đường truyền và congestion window. Không hứa “chat có độ trễ không đổi dù đang gửi file”. Cần chứng minh chat không bị buộc chờ file khôi phục dữ liệu mất.

## 3. Những phần đã làm được và còn thiếu

### 3.1 Đã có trong mã nguồn — tiếp tục phát triển, không viết lại từ đầu

- [x] Tách `common/`, `transport/`, `server/`, `client/`, `client/web_ui/`.
- [x] Header nhị phân 23 byte, encode/decode và kiểm độ dài/version.
- [x] JSON message ứng dụng; chunk file dùng bytes thô.
- [x] UDP HELLO/HELLO_REPLY; client retry; server xử lý HELLO lặp cho phiên còn tồn tại.
- [x] X25519, HKDF, AES-256-GCM cho DATA, header DATA làm AAD.
- [x] Sequence riêng từng stream; phân mảnh/ghép message theo EOM; loại DATA trùng theo `seq`.
- [x] ACK tích lũy + bitmap SACK 32 bit; cửa sổ gửi theo cwnd; nhận trong cửa sổ giới hạn seq.
- [x] Ước lượng RTT/RTO; gửi lại theo timeout; backoff RTO chung; giới hạn số lần retry.
- [x] Congestion control AIMD đơn giản; scheduler duyệt stream theo vòng trong mỗi lần pump.
- [x] Heartbeat, peer timeout, CLOSE; bắt một số lỗi packet/socket/callback.
- [x] Login đúng/sai, kiểm username đã online; Registry lưu IP/port và thời điểm hoạt động.
- [x] Chat broadcast/DM; thông báo user vào/ra; relay tín hiệu và chunk file.
- [x] Đọc file theo chunk, tính hash theo block; ghi file, kiểm SHA-256, xóa file sai hash.
- [x] HTTP server tự viết; HTML/JS; SSE; chat, danh sách user, progress cơ bản.
- [x] Nút nhận/từ chối file đã có; nội dung chat hiển thị qua `textContent`.
- [x] `GET /api/state` đã có connected/username/users; chưa có RTT/cwnd/retransmit.
- [x] `LossyLink` có seed, loss, delay, jitter và duplicate.
- [x] Test packet, crypto cơ bản, handshake, thứ tự, message lớn, loss 10%, reorder/dup, auth/chat/file cơ bản đã được viết.
- [x] Script load test dùng các tài khoản mẫu `user_001` đến `user_020`.
- [x] `server.main` đã có `--host` mặc định `0.0.0.0`; server/client đều có `-v`.
- [x] `docs/protocol.md`, dàn ý báo cáo và thư mục sơ đồ đã có khung.

Các dấu tick trên chỉ ghi nhận **có triển khai**, không phải giấy chứng nhận test pass.

### 3.2 Cần hoàn thiện

| Phần | Khoảng trống hiện tại | Làm ở mục |
|---|---|---|
| Step 2 | Chưa có client–server TCP bootstrap | 7 |
| Protocol | Tài liệu còn TODO; chưa chốt API chờ credit/ACK và message mở rộng | 6 |
| Reliability | Chưa fast retransmit; timeout retransmit có thể burst; chưa recovery epoch | 8 |
| Congestion | Chưa CUBIC; AIMD chưa nên gọi là NewReno đầy đủ | 9 |
| Flow control | Chưa credit stream/connection; pending queue có thể tăng | 10 |
| Scheduler | Chưa chính sách ưu tiên chat/control và chống starvation rõ ràng | 10 |
| File | Progress đếm bytes enqueue; chưa giới hạn toàn tuyến; lỗi file 0 byte qua mạng cần sửa | 11 |
| Relay | Chưa FILE_ERROR đầy đủ khi rớt; thiếu transfer state ràng buộc sender/receiver/accept | 11 |
| Security | SHA-256 mật khẩu chưa salt/KDF; handshake chưa xác thực; ACK/control transport chưa được xác thực | 12 |
| Resilience | Reconnect chưa có; dọn file/task/future khi lỗi chưa đầy đủ | 13 |
| UI | RTT chưa có; upload HTTP đọc cả body; chưa hủy file đang truyền | 11, 14 |
| Testing | Bốn test skip; load test chưa assert số liệu rõ ràng; chưa có bằng chứng LAN trong audit | 15 |
| Submission | README/đặc tả còn thông tin cũ; chưa có báo cáo PDF hoàn chỉnh được xác nhận | 16 |

### 3.3 Bốn test đang bỏ qua

| Test | File | Người đề xuất phụ trách |
|---|---|---|
| `test_no_head_of_line_blocking` | `tests/test_reliability.py` | A |
| `test_peer_disconnect_detected` | `tests/test_reliability.py` | A |
| `test_file_over_lossy_network` | `tests/test_file_transfer.py` | A phối hợp B |
| `test_client_crash_is_cleaned_up` | `tests/test_auth_routing.py` | B |

Không chỉ xóa decorator `skip`: phải viết thân test và assertion thật.

## 4. Bản đồ file: cần sửa gì thì mở đâu?

```text
collab-app/
├── pipeline.md                    hướng dẫn này
├── WORKPLAN.md                    phân công/lịch, cần đồng bộ lại
├── README.md                      hướng dẫn chạy cho người chấm
├── requirements.txt               cryptography, pytest
├── common/
│   ├── constants.py               loại packet, stream, giới hạn, timeout
│   ├── packet.py                  header và encode/decode
│   └── messages.py                tên/encode/decode message ứng dụng
├── transport/
│   ├── endpoint.py                socket UDP, handshake, timer, API ứng dụng
│   ├── connection.py              gửi/nhận, ACK, retransmit, scheduler
│   ├── stream.py                  queue, seq, phân mảnh, ghép message
│   ├── congestion.py              RTT/RTO, AIMD; nơi thêm CUBIC
│   └── crypto.py                  X25519/HKDF/AES-GCM
├── server/
│   ├── main.py                    CLI server
│   ├── server.py                  login và điều phối ứng dụng
│   ├── auth.py                    kiểm mật khẩu
│   ├── registry.py                bảng user online
│   ├── router.py                  chat và relay file
│   └── data/users.json            tài khoản demo
├── client/
│   ├── main.py                    CLI daemon/UI
│   ├── client.py                  login/chat/file, events, lifecycle
│   ├── file_transfer.py           đọc/ghi/hash file
│   └── web_ui/
│       ├── http_server.py         HTTP routes, upload, SSE
│       └── static/                index.html, app.js, style.css
├── tests/
│   ├── lossy_socket.py            LossyLink; không phải transport/lossy.py
│   ├── test_packet.py
│   ├── test_reliability.py
│   ├── test_auth_routing.py
│   ├── test_file_transfer.py
│   └── load_test.py
├── docs/
│   ├── protocol.md
│   ├── report_outline.md
│   └── diagrams/
└── reference/                     tài liệu/code tham khảo, không phải spec
```

Các file như `transport/bootstrap.py`, `tests/test_bootstrap.py`, `tests/test_security.py`, `docs/evidence/`, `docs/AUDIT_A.md` và `docs/AUDIT_B.md` ở các bước dưới là **đề xuất tạo mới**, không phải đã có. Không tham chiếu `Assignment1_Context.md` vì chưa tìm thấy file này trong repo khi rà soát.

## 5. Bước chuẩn bị — chạy được bản hiện tại trước

**Mục tiêu:** có môi trường thống nhất và biết lỗi nào đã tồn tại trước khi sửa.

### 5.1 Cài trên Windows PowerShell

Mở terminal tại thư mục chứa `pipeline.md`. Chạy từng dòng, đợi xong rồi mới chạy dòng tiếp theo:

```powershell
python --version
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m pytest -q -rs
```

Dự án hiện ghi Python 3.10+. Ghi phiên bản thực tế dùng trong README và log test. Các lệnh trên dùng thẳng Python của venv nên không cần sửa execution policy hay activate PowerShell.

Nếu báo thiếu `cryptography`, kiểm tra đã chạy pip bằng đúng `.venv\Scripts\python.exe` chưa. Không cài vào một Python rồi test bằng Python khác.

Trên macOS/Linux, các lệnh tương đương:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pytest -q -rs
```

Không cần thêm `pytest-asyncio` cho các test hiện tại vì chúng dùng `asyncio.run()`. Chỉ thêm dependency khi thực sự dùng.

### 5.2 Chạy hai client bằng ba terminal

Terminal 1:

```powershell
.\.venv\Scripts\python.exe -m server.main --host 0.0.0.0 --port 9000 -v
```

Terminal 2:

```powershell
.\.venv\Scripts\python.exe -m client.main --server 127.0.0.1:9000 --ui-port 8080 -v
```

Terminal 3:

```powershell
.\.venv\Scripts\python.exe -m client.main --server 127.0.0.1:9000 --ui-port 8081 -v
```

1. Mở `http://127.0.0.1:8080`, đăng nhập `alice / alice123`.
2. Mở `http://127.0.0.1:8081`, đăng nhập `bob / bob123`.
3. Kiểm tra mỗi cửa sổ thấy người kia online.
4. Gửi chat chung. Bấm tên người nhận rồi gửi DM.
5. Chọn người nhận, gửi file nhỏ, bấm Nhận ở cửa sổ kia.
6. So sánh nội dung file trong thư mục download. Thử thêm nút Từ chối.
7. Ghi thao tác nào đạt, thao tác nào lỗi; không dùng ký ức “lần trước chạy được” thay log.

### 5.3 Lưu baseline và chuẩn bị cộng tác

1. Tạo `docs/evidence/` để lưu log và số liệu; tránh lưu mật khẩu thật.
2. Ghi ngày, OS, Python, lệnh test, số pass/fail/skip và lỗi đầu tiên.
3. Kiểm tra `git status`. Lần rà soát trước thư mục này không có `.git`. Nếu là ZIP giải nén thì clone repo đúng của nhóm hoặc khởi tạo repo mới theo lựa chọn của nhóm trước khi dùng quy trình PR.
4. Bảo đảm `.gitignore` bỏ venv, cache, downloads, file tạm. Không xóa dữ liệu chỉ để làm sạch trạng thái Git.
5. Chốt A phụ trách transport, B phụ trách app/UI; ai sửa `common/constants.py` hoặc API chung phải báo người kia.
6. Nếu OneDrive gây khóa/sync file, làm việc trên bản sao ngoài OneDrive rồi kiểm tra đủ file. Không tự di chuyển/xóa thư mục gốc khi chưa có bản sao an toàn.

**Xong bước này khi:** cả hai chạy được cùng lệnh, có kết quả baseline và danh sách lỗi thực tế.

## 6. Bước thiết kế — chốt protocol và API trước khi sửa tiếp

**Phụ trách:** A viết transport; B viết message, file state và UI events. **Rubric:** Step 1.

### 6.1 Ghi đúng protocol hiện có

Mở `common/packet.py`, `common/constants.py`, `transport/stream.py`, rồi cập nhật `docs/protocol.md`:

- Header `!BBBHIIIIH`, 23 byte: version, ptype, flags, stream_id, conn_id, seq, ack, sack, length.
- `seq` thuộc từng stream, không phải packet number toàn connection.
- ACK = `seq` tiếp theo đang chờ; mọi `seq < ack` đã nhận. Bit SACK i tương ứng `ack + 1 + i`.
- Bitmap chỉ biểu diễn 32 vị trí phía sau lỗ hổng; cửa sổ hiện là 64 packet. Giải thích giới hạn này hoặc sửa đồng bộ khi thay đổi.
- Một message có thể phân mảnh; packet cuối gắn EOM. Chat và file phải có giới hạn kích thước message.
- Nonce hiện là `stream_id(2 byte) || seq(4 byte) || 6 byte 0`, khóa riêng mỗi chiều.
- DATA được mã hóa; ACK/PING/PONG/CLOSE hiện chưa có xác thực mật mã.

**Lựa chọn kế hoạch:** tiếp tục dùng `seq` theo stream. Không bắt buộc đổi sang QUIC packet number/byte offset. Nếu sau này chọn đổi, đó là một thay đổi protocol riêng cần version mới, cập nhật ACK/crypto/test và dự toán lại.

### 6.2 Chốt giao diện mà B thực sự gọi

API hiện tại là `Endpoint`, không chỉ `Connection`/`Stream`:

```python
await endpoint.start(local_addr)
conn_id = await endpoint.connect(server_addr)
stream_id = endpoint.open_stream(conn_id)
ok = endpoint.send(conn_id, stream_id, data)
endpoint.close(conn_id)
await endpoint.stop()
```

Callbacks hiện đồng bộ: `on_connect`, `on_receive`, `on_disconnect`. `send()` thành công mới chỉ có nghĩa transport đã nhận yêu cầu enqueue, chưa nghĩa đã ACK hoặc giao tới người nhận cuối.

Trước khi B viết backpressure/progress, A và B phải ghi chữ ký API mới cho bốn nhu cầu:

1. Gửi có chờ khi queue/credit đầy, ví dụ `await send_message(...)`; đây là API dự kiến.
2. Nhận thông báo bytes DATA được ACK mới, không đếm trùng retransmit.
3. Báo dữ liệu đã được ứng dụng tiêu thụ để trả credit.
4. Hủy stream/transfer và đánh thức mọi tác vụ đang chờ khi connection đóng.

Định nghĩa rõ: giới hạn queue tính theo byte hay message; ai sở hữu buffer; khi đóng thì raise lỗi gì; một task gửi bị cancel có bỏ dữ liệu đã enqueue hay không. Không đổi callback sync thành async rồi gọi mà không `await`.

### 6.3 Viết các sơ đồ trước

Tạo sơ đồ connection, TCP bootstrap + UDP handshake + AUTH, chat, file có mất gói, và file bị ngắt. Dùng Mermaid trong Markdown hoặc xuất ảnh; tên trạng thái phải khớp code cuối.

Với file, tối thiểu mô tả:

```text
OFFERED → ACCEPTED → TRANSFERRING → VERIFYING → DONE
    └→ REJECTED          └→ FAILED/CANCELLED
```

Ghi rõ message nào chuyển trạng thái và ai được phép gửi. Không cho `FILE_BEGIN` tự tạo quyền truyền nếu chưa có OFFER/ACCEPT hợp lệ.

**Xong bước này khi:** người B có thể viết app theo API đã ghi mà không đọc chi tiết thuật toán của A; đặc tả phân biệt rõ “hiện tại” và “sắp thêm”.

## 7. Bước TCP bootstrap — đáp ứng Step 2

**Phụ trách:** A; B tích hợp lifecycle. **File dự kiến:** `transport/bootstrap.py`, `server/server.py`, `client/client.py`, hai file `main.py`, `tests/test_bootstrap.py`.

1. Thêm TCP listener ở server. Đề xuất dùng cùng số cổng 9000 với UDP: TCP và UDP là hai transport khác nhau. Nếu chọn cổng khác, thêm tham số CLI và ghi rõ cả hai cổng trong README.
2. Chốt framing trước: ví dụ một JSON UTF-8 kết thúc bằng newline, giới hạn 4 KiB cho mỗi message bootstrap. TCP là byte stream, một lần `read()` không nhất thiết trả nguyên một JSON.
3. Client mở TCP tới server; gửi yêu cầu chứa version. Server kiểm version rồi trả một ACK ứng dụng, ví dụ bên dưới.
4. Client đọc đủ một response, kiểm type/version/udp_port hợp lệ rồi đóng TCP.
5. Client thực hiện UDP HELLO/HELLO_REPLY tới cổng được trả về. Không gửi chat/file qua TCP bootstrap.
6. Đặt timeout connect/read, giới hạn kích thước request và luôn đóng writer khi lỗi. Đóng listener khi server dừng.
7. Tích hợp bootstrap vào đường khởi động thật của client; không chỉ viết một script demo không được ứng dụng sử dụng.

Ví dụ **message dự kiến**, chưa có trong code:

```json
{"type":"BOOTSTRAP_ACK","version":1,"udp_port":9000}
```

Không cần trả public key qua TCP để đáp ứng Step 2. Khóa UDP đã trao đổi ở HELLO. Nếu bổ sung token sau này thì phải định nghĩa kiểm tra token/hết hạn; không thêm một trường token rồi không sử dụng.

**Kiểm tra:** client nhận ACK từ server thật; response bị chia thành nhiều lần ghi vẫn đọc được; sai version, JSON hỏng và timeout được xử lý; bootstrap xong login/chat UDP vẫn hoạt động. Phân biệt TCP ACK của hệ điều hành với message ACK mà test ứng dụng cần quan sát.

**Xong khi:** có test Step 2 chạy đạt và sơ đồ chứng minh client daemon kết nối TCP tới server, không nhầm với browser–daemon.

## 8. Bước reliability — gửi lại đúng phần thiếu

**Phụ trách:** A. **File:** `transport/stream.py`, `transport/connection.py`, `transport/congestion.py`, `tests/test_reliability.py`.

### 8.1 Làm từng thay đổi nhỏ

1. Đọc `SendStream.unacked`, `on_ack()`, `timed_out()` và `Connection.on_tick()`. Vẽ một ví dụ: gửi seq 0,1,2,3,4; mất seq 1; nhận 0,2,3,4.
2. Viết test tình huống đó: ACK tích lũy dừng ở 1, SACK thể hiện 2,3,4 đã đến; ứng dụng chỉ giao đúng thứ tự, đúng một lần.
3. Thêm fast retransmit khi có đủ bằng chứng các seq phía sau đã nhận. Nếu chọn ngưỡng 3 packet, đếm **các seq khác nhau được xác nhận**, không đếm ba bản sao của cùng một ACK.
4. Đánh dấu lần recovery để ACK cũ/lặp không liên tục gửi lại cùng lỗ hổng.
5. Giữ timeout làm phương án khi mất packet cuối hoặc mất ACK, không có đủ packet phía sau để fast retransmit.
6. Tổ chức retransmit thành hàng đợi có ngân sách gửi/pacing. Không gửi toàn bộ gói timeout trong một lượt gây burst; cũng không chặn mọi retransmit chỉ vì cwnd đã giảm dưới số gói đang bay, khiến kết nối không phục hồi được.
7. Chốt timer theo packet/stream và exponential backoff có giới hạn. Một batch timeout không nhân đôi RTO nhiều lần chỉ vì có nhiều packet mất.
8. Chỉ lấy mẫu RTT từ packet chưa retransmit với thiết kế dùng lại seq hiện tại. Đây là điều code đang làm; không xóa quy tắc đó.
9. Thêm counters: bytes DATA mới, retransmit, ACK mới, timeout; dùng để giải thích kết quả.

Không phải mọi cơ chế của QUIC đều cần đưa vào. Nếu chọn RTO + fast retransmit thì ghi đúng như vậy; không ghi đã có PTO hay time-threshold loss khi chưa triển khai. Nếu thêm time-threshold, đặc tả rõ nó khác timer timeout như thế nào.

### 8.2 Trường hợp dễ bỏ sót

- ACK bị mất: receiver nhận DATA trùng phải ACK lại, nhưng không giao message lần hai.
- ACK/SACK không hợp lệ: không được giải phóng packet chưa từng gửi hay tăng cwnd vô hạn.
- Seq tăng đến giới hạn field: đóng/tạo phiên mới trước khi wrap; không âm thầm quay về 0 với cùng khóa.
- Message không có EOM: `_fragments` không được tích lũy vô hạn; giới hạn theo loại stream/message và đóng hoặc báo lỗi khi vượt mức.
- Khi connection đóng, giải phóng pending/unacked/timer và báo lỗi cho người đang chờ.

**Kiểm tra:** loss DATA, loss ACK, reorder, duplicate, mất packet cuối, mất retransmit, hết retry. So nội dung và số lần giao message; timeout của test phải hữu hạn.

**Xong khi:** message/file nhỏ qua loss 10% hoạt động với seed ghi lại; log cho thấy phần thiếu được gửi lại, không gửi lại toàn bộ message theo mỗi ACK lặp.

## 9. Bước congestion control — hoàn thiện CUBIC

**Phụ trách:** A chốt interface/recovery; B có thể triển khai CUBIC sau đó. **File:** `transport/congestion.py`, `transport/connection.py`, test congestion mới nếu cần.

1. Giữ AIMD làm baseline để so sánh; ghi tên thuật toán đang bật trong cấu hình/log.
2. Chốt đơn vị của `cwnd` và `ssthresh`: hiện là số packet. Không trộn với byte khi thêm thuật toán mới.
3. Chốt interface `on_ack`, `on_loss`, `on_timeout`, ngân sách gửi, và cách lấy thời gian monotonic. CUBIC cần thông tin thời gian, không chỉ số lượng ACK.
4. Đọc đặc tả CUBIC chính thức trước khi triển khai; ghi nguồn/version trong báo cáo. Không chỉ gắn tên CUBIC lên công thức tăng/giảm AIMD.
5. Viết slow start, cập nhật cửa sổ theo CUBIC, xử lý loss/recovery và timeout; có giới hạn min/max phù hợp thử nghiệm.
6. Dùng recovery epoch để một đợt mất gói không gây giảm cửa sổ cho từng packet. Câu “một lần mỗi RTT” chỉ là cách giải thích ngắn; điều kiện trong code phải dựa trên ranh giới recovery được ghi rõ.
7. Ghi log thời gian, cwnd, inflight, ACK, loss. Vẽ biểu đồ bằng công cụ phù hợp nếu cần; thêm `matplotlib` chỉ khi dùng script đó.

**Kiểm tra bằng thời gian giả/lịch sự kiện cố định:** ACK làm cửa sổ tiến triển; loss giảm đúng một lần trong đợt recovery; timeout có phản ứng; cwnd không âm; gửi không vượt ngân sách định nghĩa. Sau đó chạy cùng kịch bản mạng để so sánh với AIMD.

**Xong khi:** thuật toán có triển khai/test thật, được bật trong cấu hình demo và báo cáo mô tả đúng. Biểu đồ đẹp không thay thế việc kiểm tra hành vi.

## 10. Bước flow control và scheduler — không để RAM tăng theo kích thước file

**Phụ trách:** A làm transport; B làm relay/producer theo API đã chốt.

### 10.1 Phân biệt hai giới hạn

- `cwnd` bảo vệ đường truyền; không biết ứng dụng nhận có đang ghi đĩa chậm hay không.
- Receive credit bảo vệ bộ nhớ bên nhận. Chỉ được cấp thêm khi dữ liệu được tiêu thụ hoặc chuyển vào vùng lưu trữ có giới hạn được quản lý.
- Giới hạn cửa sổ seq hiện tại không tự giới hạn `pending`, message fragments, queue relay hay upload HTTP.

### 10.2 Triển khai theo thứ tự

1. Liệt kê các buffer: HTTP upload, send pending, unacked, receive reorder, fragments, relay queue, UI events. Chọn giới hạn và đơn vị cho từng loại.
2. Thêm credit per-stream và per-connection; đặc tả cách biểu diễn trong protocol trước khi sửa packet. Tên frame có thể do nhóm chọn, không bắt buộc giống QUIC.
3. Dùng cập nhật credit có tính lặp an toàn, chẳng hạn giới hạn tích lũy chỉ tăng; packet credit bị trùng không được cấp thêm dung lượng lần nữa. Có cách gửi lại/probe khi update bị mất để tránh deadlock.
4. Sender chỉ đưa dữ liệu mới ra mạng khi còn cả cwnd, credit stream và credit connection. Retransmit cùng dữ liệu không trừ receive credit lần hai.
5. Producer file phải `await` khi queue đầy. `sleep(0.01)` sau 64 chunk hiện tại chỉ nhường event loop, không phải backpressure.
6. Receiver giữ chỗ cho dữ liệu đã ACK; nếu thiếu chỗ thì không ACK một dữ liệu sẽ bị vứt đi rồi khiến sender tưởng đã giao xong.
7. Callback nhận sync không được block chờ B. Thiết kế queue có giới hạn được gắn với receive credit, hoặc chuyển sang API nhận async có consumer xác định. Chốt việc này với B trước khi triển khai.
8. Server nhận từ A, relay sang B: nếu B hết credit thì queue relay đầy; ngừng cấp thêm credit file cho A. Server không nhận vô hạn rồi hy vọng gửi kịp.
9. Dành ngân sách cho control/chat để file không chiếm hết connection credit. Message ACK/credit cần đường tiến triển để không tạo vòng chờ.
10. Scheduler ưu tiên control/chat với ngân sách rõ ràng; các file round-robin có con trỏ qua nhiều lần pump. Giới hạn ưu tiên để chat liên tục không làm file bị bỏ đói.

**Kiểm tra:** làm receiver tiêu thụ chậm; sender phải chờ, các queue đạt trần rồi ổn định. Tăng file từ 10 MB lên 100 MB không làm mức RAM đỉnh tăng tương ứng với toàn file. Đóng connection trong lúc chờ credit phải kết thúc task, không treo.

**Kiểm tra HoL:** cố ý bỏ một packet stream file, cho chat ở stream 1 đi qua, chứng minh chat được giao trước khi lỗ hổng file được khôi phục. Đo thêm p50/p95 chat latency có/không truyền file; ghi điều kiện mạng và ngưỡng chấp nhận trước khi chạy. Không ghi “độ trễ gần như không đổi” nếu chưa có số đo.

## 11. Bước file transfer — hoàn thiện toàn tuyến A → server → B

**Phụ trách:** B, phối hợp A về credit/ACK. **File:** `client/file_transfer.py`, `client/client.py`, `server/router.py`, `common/messages.py`, HTTP UI và các test file.

### 11.1 Quản lý transfer thành một trạng thái rõ ràng

1. Server lưu transfer ID, sender, receiver, size, hash, trạng thái, stream hai chặng, bytes và thời điểm hoạt động. Khóa tra cứu phải tránh nhầm file ID từ người khác.
2. OFFER phải hợp lệ: người nhận online, size không âm, tên/hash đúng kiểu, không vượt giới hạn cấu hình.
3. Chỉ receiver chỉ định mới được ACCEPT/REJECT; chỉ sender tương ứng mới được BEGIN/chunk; chỉ receiver mới được DONE.
4. Chặn ACCEPT lặp tạo nhiều task gửi cùng file. Chặn BEGIN không có OFFER hoặc chưa ACCEPT.
5. Không giả định thứ tự giữa stream control và stream file; chỉ dựa vào trạng thái đã xác nhận và thứ tự nội bộ mỗi stream.
6. Khi relay đã enqueue hết byte, chưa được coi cả transfer đã thành công: vẫn giữ metadata đến DONE/lỗi/timeout để báo đúng cho hai bên.

### 11.2 Sửa file rỗng và kiểm số byte

1. Unit test file 0 byte đã có, nhưng đường mạng hoàn tất hiện chờ `rx.write()` nhận chunk. File rỗng không có chunk nên có thể không phát DONE.
2. Khi nhận BEGIN hợp lệ với size 0, hoàn tất ngay: đóng file, so SHA-256 của dữ liệu rỗng, phát DONE và cleanup cả relay.
3. Trước mỗi lần ghi, kiểm tra tổng byte không vượt size đã khai báo. Không ghi thừa rồi mới phát hiện sau.
4. Nhận thiếu byte quá timeout phải thất bại; sai hash phải xóa file dở và thông báo.
5. Thử file 1 byte, 8192 byte, 8193 byte, nhiều chunk, Unicode/khoảng trắng và hai file cùng tên. Tên file không được thoát thư mục download; file tạm upload cần tên duy nhất.

### 11.3 Sửa bộ nhớ từ browser tới receiver

1. Giữ `FileSender.chunks()` và hash theo block đang có.
2. Trong `_send_file_data()`, thay nhịp sleep bằng API gửi có chờ queue/credit.
3. Trong `http_server.py`, xử lý route upload theo chunk đọc từ request body và ghi dần ra file tạm. Hiện `readexactly(length)` giữ toàn bộ file trong RAM, dù FileSender đã đọc theo chunk.
4. Đặt timeout, giới hạn body, kiểm Content-Length không âm, xử lý upload bị ngắt và xóa file tạm lỗi.
5. Hash/đọc/ghi file lớn không được chặn event loop quá lâu. Đưa thao tác đĩa/hash nặng vào worker phù hợp, hoặc chia nhỏ với lịch thực thi rõ ràng; vẫn giữ giới hạn bộ nhớ.
6. Dọn file tạm upload khi transfer kết thúc/hủy và khi daemon dừng.

### 11.4 Định nghĩa progress trung thực

Phân biệt bốn số:

| Số đo | Ý nghĩa |
|---|---|
| Bytes browser upload | Browser đã đưa bao nhiêu byte tới daemon cục bộ |
| Bytes enqueue | Daemon đã giao bao nhiêu byte vào transport; đây là số hiện đang hiển thị |
| Bytes ACK chặng A → server | Server đã nhận; chưa chứng minh B đã nhận |
| Bytes B nhận + hash | Tiến độ người nhận cuối; DONE chỉ sau khi hash hợp lệ |

1. Nếu dùng ACK chặng đầu, đặt nhãn “Đã gửi tới server”; chỉ đếm payload file duy nhất, không cộng header, BEGIN, tag hay retransmit.
2. Nếu muốn hiển thị “Người nhận đã nhận”, thêm message progress từ B qua server với tần suất giới hạn. Đây là mở rộng cần ghi trong `messages.py` và đặc tả.
3. Khi chặng đầu đạt 100%, hiển thị “Chờ người nhận xác minh” cho đến `FILE_DONE {ok:true}`. Không xóa progress rồi ngầm coi file đã xong.
4. Speed/ETA là tùy chọn; mẫu số file rỗng cần xử lý riêng, tránh chia cho 0.

### 11.5 Lỗi, từ chối và hủy

1. Giữ nút từ chối đã có; xác nhận từ chối không tạo luồng dữ liệu.
2. Khi một bên rớt, server gửi `FILE_ERROR` có file_id cho bên còn lại, dọn relay và trạng thái transfer.
3. Client nhận lỗi phải hủy task gửi, đóng/xóa receiver dở, dọn maps, phát event UI cho đúng file. Hiện chỉ phát dòng lỗi là chưa đủ.
4. Hủy file đang truyền là tính năng khác REJECT. Nếu làm, thêm message/state/API cancel rõ ràng; không dùng nút Từ chối để giả lập hủy sau ACCEPT.
5. Chỉ dọn tài nguyên của transfer liên quan; các file và chat khác phải tiếp tục chạy.

**Xong khi:** file thường/rỗng/lớn truyền được qua relay, RAM được giới hạn, tiến độ đúng nghĩa, lỗi có thông báo và không còn file handle/task/metadata bị bỏ quên.

## 12. Bước security — giữ mã hóa và sửa các giới hạn được phát hiện

**Phụ trách:** B; thay header/control packet cần A phối hợp. **Rubric:** Step 3 và 8.

### 12.1 Những gì cần chứng minh cho Step 8

1. Giữ X25519/HKDF/AES-GCM đang có; không tự viết thuật toán mã hóa.
2. Kiểm hai chiều dùng đúng send/recv key và header DATA được xác thực bằng AAD.
3. Test sửa bit ciphertext/tag và header làm DATA bị loại, không giao lên ứng dụng.
4. Test DATA trùng không sinh tin chat/chunk lần hai. Bảo đảm cơ chế chống trùng vẫn gửi ACK lại để mất ACK không gây kẹt.
5. Giới hạn số stream/seq theo kích thước field; kết nối lại dùng khóa mới.

### 12.2 Nonce và retransmit

Không tiếp tục ghi “nonce lấy từ packet number mới” vì code hiện không có field đó. Với thiết kế seq giữ lại:

- Mỗi cặp khóa/nonce chỉ dùng để tạo một bản mã cho cùng DATA bất biến. Không mã hóa nội dung hoặc AAD khác bằng cùng nonce.
- Hướng cải thiện: tạo và cache bytes packet DATA đã mã hóa lần đầu, retransmit nguyên bytes đó; không chỉnh flags/header/payload của bản đã gửi.
- Cache phải được giải phóng khi ACK/đóng kết nối và nằm trong ngân sách RAM.
- Nếu thêm xác thực ACK/PING/CLOSE/credit, phải thiết kế nonce/counter riêng với miền không trùng DATA, cùng quy tắc replay và retry. Không đưa chúng vào AES-GCM với `seq=0` lặp lại.

### 12.3 Những việc tăng độ chắc chắn, không tự gọi là điểm riêng trong rubric

1. Thay SHA-256 mật khẩu trần bằng PBKDF2/scrypt có salt; sửa định dạng `users.json` và tạo lại tài khoản demo tương ứng. Đây là hardening, DOCX chỉ ghi login và mã hóa payload.
2. ACK/PING/CLOSE hiện không được xác thực mật mã: bổ sung xác thực control transport để packet giả không làm tăng cửa sổ, đóng phiên hoặc thay credit. Chỉ cập nhật liveness sau khi packet hợp lệ theo cơ chế đã chọn.
3. X25519 hiện chưa xác thực danh tính server. Nếu chưa triển khai PSK/pinned key/chữ ký, ghi rõ chưa chống MITM chủ động. Không gọi challenge-response là đã có hoặc là yêu cầu nguyên văn của đề.
4. Challenge-response/replay window mở rộng chỉ triển khai sau khi có đặc tả rõ; không tự phát minh thêm handshake phức tạp ngay trước hạn nộp.
5. Giới hạn đăng nhập sai/tần suất tin là cải thiện thêm; giới hạn kích thước và số lượng tài nguyên vẫn phải có để xử lý input lỗi.

### 12.4 Bằng chứng mạng bổ trợ

1. Mở Wireshark trên loopback hoặc card LAN, filter `udp.port == 9000`.
2. Gửi một tin chat dễ nhận biết, tìm packet DATA và chụp payload đã mã hóa.
3. Lưu ảnh cùng ghi chú vào `docs/evidence/`. HELLO/metadata không nhất thiết được giấu; đừng kết luận “mọi byte trên mạng đều bí mật”.
4. Ảnh không thấy plaintext chỉ bổ trợ trình bày; không chứng minh toàn bộ giao thức an toàn và không thay test mật mã.

**Xong khi:** có test encryption/tamper thực chạy, quy tắc nonce rõ ràng, tài liệu mô tả chính xác phạm vi bảo mật và các hạn chế còn lại.

## 13. Bước resilience và quản lý user

**Phụ trách:** A làm Endpoint/Connection; B làm server/client lifecycle. **Rubric:** Step 3, 4, 7.

1. Giữ registry username → conn_id → IP/port; kiểm duplicate username và thao tác login lặp trên cùng connection. Không để một connection để lại nhiều username mồ côi.
2. Phân biệt timeout transport với `last_seen` của ứng dụng. Hiện transport dùng `time.monotonic()`, Registry dùng `time.time()`; nếu dùng giá trị nào để tính khoảng thời gian thì phải nhất quán, ưu tiên monotonic cho elapsed time.
3. Thêm giới hạn số connection chưa auth/đã auth, số stream và thời gian chờ auth. Dọn handshake cache cùng connection.
4. Packet rác/sai type/sai độ dài không được làm server ngừng phục vụ client tốt. Log có giới hạn, tránh spam mỗi datagram.
5. Kiểm đường báo lỗi UDP trên Windows: `error_received`, `sendto`, WinError 10054. Hiện đã bắt/log một số lỗi; test tình huống thật trước khi thêm code xử lý trùng.
6. Nếu tăng `SO_RCVBUF`, ghi giá trị mong muốn và đọc lại giá trị thực; đó không thay cho flow control.
7. Khi client mất kết nối: dọn transfers/tasks/futures, báo UI, xóa trạng thái online cũ.
8. Tạo đúng một task reconnect có backoff + jitter và trần thời gian chờ; stop/logout phải hủy task này. Kết nối mới phải chạy lại bootstrap, UDP handshake và auth.
9. Nếu tự login lại, giữ thông tin cần thiết trong bộ nhớ theo chính sách đã chọn, không ghi mật khẩu vào log/đĩa. Sai mật khẩu hoặc bị từ chối auth không được retry dồn dập vô hạn.
10. Mở HTTP UI ngay cả khi server chưa online để người dùng thấy trạng thái và thử lại. Không để `await client.start()` thất bại làm toàn bộ UI biến mất.
11. Reconnect không có nghĩa resume file. Mặc định đánh dấu transfer cũ thất bại; muốn gửi lại thì tạo transfer mới.
12. Khi server/daemon dừng, đóng listener TCP, UDP, HTTP/SSE và file; cancel rồi thu dọn background task.

**Kiểm tra:** kill client không LOGOUT, chặn mạng một chiều, tắt/bật server, server chưa bật khi mở client, login trùng, gửi rác khi client khác đang chat. Server phải tiếp tục phục vụ và danh sách online được dọn sau timeout cấu hình.

## 14. Bước UI — hoàn thiện những gì người dùng nhìn thấy

**Phụ trách:** B. **File:** `client/web_ui/http_server.py`, `static/app.js`, `static/index.html`, `static/style.css`.

1. Giữ các phần đã có: login, danh sách user, chọn DM, chat chung, upload, Nhận/Từ chối, SSE và progress.
2. Phân biệt trạng thái browser–daemon và daemon–server; mất SSE không đồng nghĩa UDP server đã chết.
3. Bổ sung `/api/state` với RTT, retransmit và trạng thái reconnect từ API thống kê đã chốt. Không để JS tự truy cập cấu trúc transport.
4. Hiển thị trạng thái ban đầu khi tải lại trang, không chỉ chờ một event “connected” đã phát từ trước.
5. Sửa progress theo mục 11.4; file rỗng hiển thị kết quả rõ ràng.
6. Khi gửi không thành công, HTTP API trả lỗi và UI hiển thị lỗi; không luôn trả `ok:true` trong lúc connection đã mất.
7. Thêm hủy file nếu đã triển khai protocol cancel; tốc độ/ETA và làm đẹp có thể làm sau các mục rubric.
8. Tiếp tục dùng `textContent` cho chat, username, tên file. Thử chuỗi `<script>` phải hiển thị như văn bản.
9. Kiểm SSE reconnect và cleanup. Queue sự kiện hiện dùng chung: nếu mở nhiều tab cùng một daemon, chúng có thể chia nhau lấy event. Hoặc hỗ trợ fan-out có queue giới hạn cho từng subscriber, hoặc ghi rõ chỉ một UI tab mỗi daemon.
10. HTTP parser cần timeout, giới hạn header/body và phản hồi lỗi hợp lý. Không biến parser tối giản thành một dự án HTTP framework đầy đủ.

**Xong khi:** hai người dùng thao tác được từ browser, nhìn được tin nhắn/user/progress/trạng thái mạng, và hiểu khi nào thao tác thành công hay thất bại.

## 15. Bước kiểm thử tổng hợp — lấy bằng chứng cho từng Step

### 15.1 Dùng đúng simulator đang có

Mở `tests/lossy_socket.py`. Class hiện có là:

```python
LossyLink(loss=0.10, delay=0.01, jitter=0.02, duplicate=0.05, seed=1)
```

- `loss`, `duplicate` là xác suất từ 0 đến 1.
- `delay`, `jitter` dùng **giây**, không phải millisecond.
- Jitter tạo khả năng đảo thứ tự; không có tham số `reorder_rate=0.05` để khẳng định đảo đúng 5%.
- Gắn link riêng cho mỗi chiều/endpoint, dùng seed cố định và ghi lại. Với file relay phải mô phỏng cả A–server và server–B, không chỉ test một cặp transport.
- Seed làm chuỗi ngẫu nhiên tái hiện tốt hơn, nhưng lịch event loop vẫn có thể khác. Test mất gói đích danh cần hook drop theo packet để có bằng chứng HoL/retransmit rõ ràng.
- Simulator hiện là hook trong code/test, chưa có CLI `--loss`; không ghi lệnh chạy tham số đó trước khi tự thêm.

### 15.2 Chạy test từ nhỏ tới lớn

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_packet.py -q
.\.venv\Scripts\python.exe -m pytest tests/test_reliability.py -q -rs
.\.venv\Scripts\python.exe -m pytest tests/test_file_transfer.py tests/test_auth_routing.py -q -rs
.\.venv\Scripts\python.exe -m pytest -q -rs
```

Sau khi tạo test mới, chạy đúng test đó trước rồi toàn bộ suite một lần khi tích hợp. Mỗi test mở Endpoint/server phải dọn trong `try/finally`, kể cả assertion thất bại. Một số test loss/reorder hiện chưa gọi stop rõ ràng; hoàn thiện cleanup khi sửa chúng.

| Kịch bản | Kiểm tra cụ thể |
|---|---|
| Mạng sạch | Login, chat chung/DM, file thường đạt |
| Loss 5%, 10%; thêm delay/jitter/dup | Nội dung đúng, không giao trùng, kết thúc trong timeout ghi rõ |
| Loss 30% | Stress test thêm; hoàn thành hoặc báo timeout sạch, không coi đây là ngưỡng bắt buộc DOCX |
| Mất DATA, mất ACK, mất packet cuối | Recovery tiến triển, không deadlock |
| File + chat đồng thời | Test HoL và số đo latency; không chỉ nhìn UI thấy nhanh |
| Receiver chậm | Queue/RAM có trần, sender bị backpressure |
| File 0/1/8192/8193 byte, Unicode | Đúng nội dung/hash, không kẹt DONE |
| File 10 MB rồi 100 MB | Đo thời gian, throughput, RAM; đây là cỡ test nội bộ |
| Sửa ciphertext/header, replay DATA | Không giao dữ liệu sai/trùng lên app |
| Kill sender/receiver, restart server | FILE_ERROR/cleanup/reconnect đúng, server còn sống |
| Nhiều file và user cùng lúc | Không lẫn dữ liệu/trạng thái và không đói tài nguyên vô hạn |

Không dùng test chỉ có `sleep` rồi in “OK”. Mỗi test cần assertion cho hành vi mong muốn và timeout để thất bại rõ ràng.

### 15.3 Test 20 client

Bật server ở terminal khác rồi chạy:

```powershell
.\.venv\Scripts\python.exe -m tests.load_test --server 127.0.0.1:9000 --clients 20
```

Script hiện login rồi chat ngay nên client đến sau có thể bỏ lỡ tin trước đó. Để kiểm đủ delivery, sửa như sau:

1. Kết nối và login tất cả client.
2. Chờ barrier xác nhận tất cả đã sẵn sàng và thấy đủ user.
3. Mỗi client gửi số lượng tin cố định, có ID để đếm và phát hiện trùng.
4. Chờ nhận đủ theo deadline. Nếu mỗi client gửi một broadcast và không echo sender, mỗi client khác kỳ vọng 19 tin.
5. In tổng số login thành công/thất bại, latency, expected/received/duplicate và thời gian toàn lượt.
6. Một lượt riêng cho connect/disconnect đồng thời; kiểm registry được dọn.
7. Fail bằng assertion/exit code khác 0 nếu không đạt; luôn stop client trong cleanup.

### 15.4 Test LAN hai máy

1. Máy server chạy `server.main --host 0.0.0.0 --port 9000`.
2. Dùng `ipconfig` tìm IPv4 LAN, ví dụ `192.168.1.10`.
3. Nếu firewall chặn, người quản trị máy mở rule phù hợp trên mạng tin cậy. Sau khi thêm bootstrap cùng cổng 9000, cần cả UDP và TCP:

```powershell
New-NetFirewallRule -DisplayName "CollabApp UDP 9000" -Direction Inbound -Protocol UDP -LocalPort 9000 -Profile Private -RemoteAddress LocalSubnet -Action Allow
New-NetFirewallRule -DisplayName "CollabApp TCP 9000" -Direction Inbound -Protocol TCP -LocalPort 9000 -Profile Private -RemoteAddress LocalSubnet -Action Allow
```

4. Máy client chạy với `--server 192.168.1.10:9000 --ui-port 8080`; mở browser tại `127.0.0.1:8080` trên chính máy client.
5. Không cần mở cổng HTTP UI ra LAN vì browser và daemon ở cùng máy. Không dùng `0.0.0.0` làm địa chỉ đích của client.
6. Dùng mạng cho phép các máy liên lạc, tránh guest Wi-Fi chặn thiết bị ngang hàng.
7. Thử chat hai chiều, file, từ chối, kill client. Ghi IP/cổng/OS, thời gian, hash và log.
8. Chỉ công bố hỗ trợ hệ điều hành đã kiểm chứng; đừng ghi đã test Windows/macOS/Linux nếu chỉ chạy Windows.

### 15.5 Lưu kết quả để viết báo cáo

Mỗi lượt ghi: ngày, revision code nếu có, OS/Python, số client, size file, seed, loss/delay/jitter/dup từng chặng, thời gian, bytes nhận, hash, retransmit, RAM đỉnh, kết quả. Throughput phải nói rõ tính từ lúc bắt đầu gửi dữ liệu hay từ OFFER tới DONE.

Lưu kết quả thực tế trong `docs/evidence/`; không điền số ước lượng làm số đo. So sánh before/after bằng bản baseline đã lưu, không phá code hiện tại để làm bản “trước” tạm bợ.

## 16. Bước tài liệu và nộp bài

### 16.1 Sửa tài liệu cho khớp code cuối

1. `docs/protocol.md`: header/version, loại packet/message, ACK/SACK, timer, CUBIC, flow control, nonce, states, lỗi và giới hạn. Có một packet ví dụ với field/hex khớp encoder.
2. `docs/diagrams/`: kiến trúc; TCP bootstrap + UDP handshake + auth; chat; file có mất gói; disconnect/cleanup.
3. `README.md`: Python/dependency, lệnh chạy server/client, cổng, mở browser, tài khoản demo, thư mục download, test và giới hạn đã biết.
4. Sửa câu “không dùng TCP” thành: TCP cho bootstrap và HTTP UI; chat/file dùng UDP tin cậy.
5. Không để số pass/skip cũ sau khi test thay đổi. Phân công nội bộ để trong WORKPLAN, không trộn vào hướng dẫn chạy cho người chấm.
6. Sửa comment/TODO và owner cũ trong code nếu chức năng đã làm xong; không để nút Từ chối đã có nhưng tài liệu vẫn nói chưa có.
7. Đồng bộ WORKPLAN: A8 thành công việc mặc định; CUBIC không cắt đầu tiên; phân lại bốn test skip; chốt API Endpoint; tính cả giờ A tham gia test LAN.

### 16.2 Báo cáo PDF theo đúng DOCX

- System Architecture: sơ đồ thành phần và ba loại kết nối, giải thích server relay.
- Protocol Specification: packet/message, reliability, window/flow/congestion, encryption và trạng thái.
- Sequence Diagram: vòng đời message và file transfer; thêm handshake/loss để giải thích rõ.
- Challenges & Solutions: phần phản ánh khoảng một trang, viết từ vấn đề thực tế đã gặp và cách sửa.
- Evaluation bổ sung: số liệu test, giới hạn thử nghiệm và kết quả; không thay các mục bắt buộc trên.

Nêu rõ lựa chọn TCP bootstrap + UDP khi giải thích chỗ DOCX chưa thống nhất. Nêu đúng giới hạn: server đọc plaintext để relay, handshake chưa xác thực nếu chưa bổ sung, chưa resume nếu chưa triển khai. Không quảng cáo chức năng vượt bằng chứng.

### 16.3 Kiểm tra bản nộp trên môi trường sạch

1. Tạo ZIP hoặc clone repo private sang thư mục sạch; không mang theo venv của máy đang phát triển.
2. Làm đúng từng lệnh README, không dùng bước bí mật ngoài tài liệu.
3. Chạy test, hai client, chat và file nhỏ; mở báo cáo PDF kiểm tra font/sơ đồ.
4. Bản nộp chứa đủ source client/server/common/transport/UI, dependencies, README và PDF. Kiểm tra đường dẫn tương đối không phụ thuộc đường dẫn máy của thành viên.
5. Loại venv/cache/download/file tạm/mật khẩu thật khỏi gói nộp; giữ tài khoản demo được ghi rõ. Không mặc định gộp toàn bộ `reference/` vào bản nộp; nếu sử dụng code mẫu thì ghi nguồn và tuân thủ license của mẫu.
6. Nộp **file ZIP hoặc link GitHub private** theo DOCX và cơ chế giảng viên truy cập. Kiểm tra đã cấp quyền phù hợp nếu dùng repo private.
7. Kiểm tra file/link nộp mở được. Cả hai thành viên đọc hiểu code của nhau để trình bày.

## 17. Thứ tự làm việc, chia việc và cách báo tiến độ

### 17.1 Thứ tự khuyến nghị

```text
Môi trường + baseline
→ Spec/API chung
→ TCP bootstrap
→ Reliability + CUBIC + quy tắc crypto
→ Flow control + scheduler
→ File/relay/progress/cleanup
→ Resilience + UI hoàn thiện
→ Test tổng hợp/LAN
→ README/PDF/bản nộp sạch
```

A và B có thể làm đồng thời ở các phần đã có hợp đồng API: B sửa file rỗng, transfer state, UI và password storage trong khi A làm transport. Backpressure/progress cần A chốt API trước; không làm hai phiên bản API khác nhau rồi ghép sát hạn.

### 17.2 Chia việc gợi ý

| Người | Phần chính | Điểm cần phối hợp |
|---|---|---|
| A | packet/stream/connection/endpoint, bootstrap, reliability, flow control, scheduler, test transport, đặc tả transport | Với B về crypto, CUBIC và API gửi/ACK/credit |
| B | auth/registry/router/client/file/UI, test app/load, README và ghép báo cáo | Với A về backpressure, cancel và file qua loss |
| Cả hai | Chốt protocol, review, test LAN, kiểm máy sạch, vấn đáp | Có giờ chung và ghi kết quả |

Nếu B làm CUBIC trong file do A quản lý, bàn giao phạm vi và interface trước; không sửa chồng lên nhau.

WORKPLAN cũ cộng A = 43 giờ, B = 50 giờ là đúng số học nhưng A còn khoảng 3 giờ tham gia B10, chưa kể review/họp/sửa lỗi. Các thay đổi TCP/CUBIC/credit có thể tăng công; phải ước lượng lại sau baseline, không coi 44 giờ/người đã đủ chắc chắn.

Ngày audit 07/11 trong WORKPLAN đã qua tại thời điểm viết tài liệu này. Không tự đánh dấu audit hoàn thành vì quá hạn; điền kết quả thực tế rồi dời mốc còn lại. Mốc “mọi Step chạy được” chỉ đặt sau khi bootstrap, file, UI và test cơ bản đều có bằng chứng.

### 17.3 Nếu không đủ thời gian

Cắt trước: làm đẹp UI, ETA/biểu đồ phụ, resume, P2P, E2E, thống kê nâng cao, challenge-response mở rộng. Không cắt yêu cầu DOCX như TCP Step 2, UDP file tin cậy, login/user discovery, async concurrency, HTTP UI, mã hóa payload và congestion control được nêu. Giới hạn bộ nhớ/cleanup/test phải đủ để chứng minh ứng dụng hoạt động ổn định.

Không đặt mục tiêu viết QUIC đầy đủ. Hoàn thiện protocol tự xây với phạm vi đã chốt và tài liệu đúng với code.

### 17.4 Mẫu ghi một công việc đã hoàn thành

```text
Ngày / người làm:
Task và Step liên quan:
Hành vi trước khi sửa:
File đã sửa:
Hành vi sau khi sửa:
Lệnh/kịch bản kiểm tra:
Kết quả thực tế và đường dẫn log:
Giới hạn/lỗi còn lại:
API nào thay đổi, người kia đã cập nhật chưa:
```

## 18. Checklist cuối cùng

Chỉ tick sau khi chạy hoặc kiểm tra có bằng chứng:

- [ ] Môi trường sạch cài được từ `requirements.txt`.
- [ ] Đặc tả và sơ đồ khớp code cuối, phân biệt yêu cầu với lựa chọn triển khai.
- [ ] Client daemon TCP connect tới server và nhận ACK ứng dụng.
- [ ] UDP handshake, login đúng/sai/trùng và bảng IP/port hoạt động.
- [ ] 20 client kết nối/ngắt/chat có số liệu, assertion và cleanup.
- [ ] Broadcast/DM tới đúng người, không lẫn hoặc giao trùng.
- [ ] File qua UDP, kể cả 0 byte và mạng có loss, đúng size/hash.
- [ ] Fast retransmit/timeout/CUBIC hoạt động và có test.
- [ ] Flow control/backpressure giới hạn RAM toàn tuyến, chat không chờ ghép file.
- [ ] Mất mạng/kill/restart có báo trạng thái, dọn phiên/file/task đúng.
- [ ] Payload mã hóa, test tamper đạt, nonce/replay được mô tả đúng.
- [ ] UI có message/user/progress/network status; không báo xong trước DONE.
- [ ] Bốn test skip đã có thân test thật; mọi skip còn lại có lý do cụ thể được đánh giá.
- [ ] Có log test tổng hợp, LAN và bằng chứng tương ứng rubric.
- [ ] README copy-paste chạy được; không còn tuyên bố tính năng chưa triển khai.
- [ ] Báo cáo PDF đủ mục DOCX; bản nộp ZIP/private repo mở được.

**Việc nên bắt đầu ngay:** làm mục 5 để có baseline thật, rồi mục 6 để chốt API. Sau đó mới chia các sửa đổi transport và app; không tiếp tục sửa theo những giả định “pipeline nói đã có” của bản cũ.
