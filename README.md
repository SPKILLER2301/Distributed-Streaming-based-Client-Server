# Collab App – Distributed Streaming-based Client-Server Collaboration

Ứng dụng chat + truyền file chạy trên **UDP tin cậy tự xây** (không dùng TCP/HTTP framework/WebSocket).
Python 3.10+.

## Cài đặt & chạy

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Terminal 1 – server
python -m server.main --port 9000

# Terminal 2, 3, ... – mỗi client một cổng giao diện khác nhau
python -m client.main --server 127.0.0.1:9000 --ui-port 8080
python -m client.main --server 127.0.0.1:9000 --ui-port 8081
# Mở trình duyệt: http://127.0.0.1:8080  (tài khoản mẫu: alice / alice123, bob / bob123 ...)
```

Chạy test: `pytest -q` (từ thư mục gốc dự án).

## Cấu trúc & người phụ trách

| Thư mục / file | Vai trò | Người |
|---|---|---|
| `docs/protocol.md` | Đặc tả giao thức (Bước 1) | A+B |
| `common/constants.py` | Hằng số dùng chung | A+B (sửa phải báo nhau) |
| `common/packet.py` | Header nhị phân tầng transport | A |
| `common/messages.py` | Message JSON tầng ứng dụng | B |
| `transport/*` | UDP tin cậy: handshake, ACK, window, stream, congestion, crypto | A |
| `client/file_transfer.py` | Chia chunk, SHA-256, ghép file | A |
| `server/*`, `client/client.py`, `client/main.py` | Logic ứng dụng | B |
| `client/web_ui/*` | Giao diện HTTP + JavaScript (Bước 9) | B |
| `tests/*` | Kiểm thử (Bước 10) | A: transport/file – B: app/load |

## Trạng thái khung code

Khung đã **chạy được đầu cuối** (đã kiểm thử: 22 test qua, 4 test `skip` là TODO; 20 client đồng thời):
handshake X25519 + mã hóa AES-GCM, ACK/SACK, cửa sổ cwnd, nhiều stream độc lập, gửi lại theo timeout,
keepalive/timeout, đăng nhập, chat broadcast/riêng, truyền file qua server có kiểm SHA-256, giao diện web.
Phần còn thiếu được đánh dấu `TODO(A)` / `TODO(B)`. Tìm nhanh: `grep -rn "TODO(" --include=*.py --include=*.js .`

Tài khoản mẫu: `alice/alice123`, `bob/bob123`, `carol/carol123`, `dave/dave123`, và `user_001..user_020` (mật khẩu = tên + `123`).

### Việc còn lại của A (Transport)
- `transport/connection.py`: **fast retransmit** theo SACK, giới hạn gửi lại theo cwnd, backoff theo từng gói (Bước 6, 7)
- `transport/congestion.py`: kiểm tra/chỉnh AIMD, tùy chọn nâng lên CUBIC
- `client/file_transfer.py`: tiến trình theo gói đã ACK, giới hạn bộ nhớ khi gửi file lớn (Bước 6)
- `tests/`: viết nốt test HoL blocking và phát hiện ngắt kết nối; thêm test file qua link mất gói (Bước 10)
- `transport/endpoint.py`: giới hạn số kết nối, lỗi UDP riêng của Windows
- `docs/protocol.md`, `docs/diagrams/`: hoàn thiện đặc tả (cửa sổ, RTO, congestion) và sơ đồ trạng thái

### Việc còn lại của B (Application)
- `server/router.py`: thông báo FILE_ERROR cho đối phương khi một bên rớt giữa chừng
- `client/client.py`: tự kết nối lại + đăng nhập lại khi mất kết nối (Bước 7)
- `client/web_ui/*`: hiển thị RTT/trạng thái mạng, nút từ chối/hủy file, hoàn thiện giao diện (Bước 9)
- `server/server.py`: chống spam, thống kê client online
- `tests/test_auth_routing.py`, `tests/load_test.py`: test client rớt đột ngột, in thống kê (Bước 10)
- README (bản cuối), báo cáo PDF (`docs/report_outline.md` là dàn ý)

## Lưu ý quy định đề bài
- Chỉ dùng `socket`/`asyncio` (raw UDP). Giao diện dùng HTTP server tự viết (`client/web_ui/http_server.py`) –
  **hãy hỏi giảng viên** xem `asyncio.start_server` (TCP) cho UI cục bộ có được chấp nhận không.
- Không dùng Flask/Django/WebSocket.
