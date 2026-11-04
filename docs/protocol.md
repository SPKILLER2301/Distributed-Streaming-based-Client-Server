# Đặc tả giao thức  [A+B – Bước 1]

> Hoàn thiện tài liệu này TRƯỚC khi code thêm. Mọi thay đổi header/message phải báo người kia.

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
- `>=2` = mỗi file một stream riêng → mất gói của file **không** chặn chat (không HoL blocking)
- Mỗi stream có không gian `seq` và cửa sổ riêng; congestion control dùng chung cho cả kết nối.

### 1.3b Message-oriented
`send(data)` được cắt thành các gói ≤ `MAX_PAYLOAD`; gói cuối có cờ EOM. Bên nhận ghép lại và giao **nguyên một message**.

### 1.4 Handshake (Bước 2)
```
Client                                   Server
  | HELLO(payload = X25519 pub key) -->   |
  |   <-- HELLO_REPLY(conn_id, server pub)|
  | (cả hai: HKDF -> khóa c2s, s2c)       |
  | DATA(stream 0, AUTH) -------------->  |   (đã mã hóa AES-GCM)
```

### 1.5 Mã hóa (Bước 8)
AES-GCM; nonce = stream_id‖seq (không trùng trong cùng 1 khóa/chiều); header là AAD → sửa header bị phát hiện.

### 1.6 Sơ đồ trạng thái kết nối  *(TODO(A): vẽ vào `docs/diagrams/state_diagram.png`)*
`CLOSED → HELLO_SENT → ESTABLISHED → CLOSING → CLOSED` (+ nhánh timeout)

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
*(TODO(A): mô tả cửa sổ, công thức RTO, AIMD/CUBIC đã chọn, quy tắc gửi lại.)*
