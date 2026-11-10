# Bằng chứng A1 — 09/11/2026

- Ngày theo Asia/Saigon; Windows PowerShell, Python **3.14.4** từ `.venv/Scripts/python.exe`.
- Revision code trước A1: `93812e930cc0cc8dc1987a4f6d47c0f4d7bac220`; working tree sạch trước khi sửa tài liệu.
- Đây là bản ghi kết quả tool thực tế, không phải raw stdout đầy đủ; không có số đo LAN/browser/RAM/20 client.

## 1. Toàn suite, trước thay đổi tài liệu

Lệnh:

```powershell
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -m pytest -q -rs
```

Kết quả: exit code **1**, `1 failed, 21 passed, 4 skipped, 2 warnings in 2.98s`.

Fail: `tests/test_auth_routing.py::test_login_broadcast_private_end_to_end`, assertion dòng 60 không tìm thấy event chat broadcast từ bob trong ev_a. Test chờ `events.qsize() >= 3`, không chờ đúng chat event. Đọc code thấy events còn chứa status/users nên điều kiện này có thể thỏa trước khi chat tới: **nghi vấn race trong test, chưa xác nhận nguyên nhân cuối cùng**. Không kết luận transport làm mất dữ liệu chỉ từ failure này; giao B6 điều tra, sửa predicate chờ và cleanup.

Warnings: PytestCacheWarning không tạo được `.pytest_cache/v/cache/nodeids` và `lastfailed`, WinError 183. Không xóa cache/dữ liệu người dùng để xử lý. Lượt test focused tắt cacheprovider để tách cảnh báo cache khỏi kết quả chức năng.

Skip thực tế:

| Test | Owner theo WORKPLAN |
|---|---|
| test_no_head_of_line_blocking | A7 |
| test_peer_disconnect_detected | A7 |
| test_file_over_lossy_network | B6 (reason TODO(A) hiện cũ) |
| test_client_crash_is_cleaned_up | B6 |

## 2. Packet/transport riêng

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_packet.py tests/test_reliability.py -q -rs -p no:cacheprovider
```

Kết quả: exit code **0**, `11 passed, 2 skipped in 2.85s`, không cảnh báo trong output lượt này. Hai skip là HoL/disconnect nêu trên.

Kịch bản loss có trong suite: `LossyLink(loss=0.10, seed=1)` hai endpoint, payload 200000 byte; reorder/duplicate: jitter=0.03 giây, duplicate=0.1, seed=2 mỗi endpoint. Đây là cặp transport loopback, **không phải file qua relay hai chặng**. Không có counters/latency/RAM lưu trong test hiện tại; không suy ra throughput hay trần RAM từ kết quả pass.

## 3. Giới hạn kết luận

Các run xác nhận baseline hiện tại và phần transport test đang thực thi. Chưa chứng minh fast retransmit, CUBIC, flow control, TCP bootstrap, control authentication hoặc HoL/disconnect vì chưa triển khai/test đầy đủ. Không có thay đổi runtime trong A1, nên không sửa failure app ở task này.

## 4. Kiểm tra tài liệu sau chỉnh sửa

- Dùng encoder hiện tại sinh `encode(Packet(PKT_PING, conn_id=7))`: đúng 23 byte, hex `0104000000000000070000000000000000000000000000`, khớp ví dụ protocol.
- Kiểm các Markdown local link trong 5 file A1: không có đường dẫn hỏng.
- Code fence đóng/mở đủ cặp; sơ đồ Mermaid đã rà soát source, chưa render thành PNG/PDF.
- `git diff --check` không báo lỗi whitespace. Chỉ thay đổi/thêm tài liệu, không thay runtime hoặc test.
