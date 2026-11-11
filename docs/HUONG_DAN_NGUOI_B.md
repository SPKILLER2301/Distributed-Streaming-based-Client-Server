# Hướng dẫn hoàn thiện phần người B

Bạn phụ trách tầng ứng dụng theo WORKPLAN: đăng nhập, danh sách người dùng, định tuyến chat, truyền file qua server, reconnect, HTTP UI, kiểm thử ứng dụng và hồ sơ nộp. Bản cập nhật này đã triển khai các phần có thể kiểm tra với transport hiện tại. Chưa thể gọi là hoàn thành toàn bộ B vì còn cần API và mã transport của A.

## 1. Các việc cần làm và trạng thái

| Mục | Đã bổ sung trong bản cập nhật | Bạn cần hoàn tất tiếp |
|---|---|---|
| B1 | Kiểm tra schema/message, trạng thái file; điểm tích hợp TCP bootstrap trong server/client; test thứ tự gọi TCP trước UDP | Nhận module `transport/bootstrap.py` của A, chạy TCP ACK thật và kiểm tra reconnect qua TCP. Test hiện tại ở điểm nối dùng stub |
| B2 | PBKDF2 có salt, kiểm tra sai/trùng/lặp đăng nhập, quản lý user–IP–port, chat riêng đúng người, timeout xác thực | Review với giới hạn endpoint của A, chạy demo nhiều terminal |
| B3 | OFFER–ACCEPT–BEGIN–DONE đúng vai trò; file rỗng, tên Unicode, size/hash; lỗi/ngắt thì dọn file dở; progress phân biệt enqueue với DONE | Nhận A5: async send/receive+consume, queue có trần, cancel_stream. Chuyển relay sang API mới rồi kiểm file 10/100 MB và receiver chậm |
| B4 | Một reconnect task có backoff/jitter, đăng nhập lại bằng thông tin trong RAM; stop/logout hủy retry; không tự resume file | Kiểm với A2/A6 thật; kiểm server restart, mạng mất lâu và LAN |
| B5 | Upload đọc từng 64 KiB, giới hạn/timeout, không đè file trùng tên, dọn upload dở; SSE một tab/daemon; HTTP và UDP có trạng thái riêng | Nhận `get_stats` của A để có RTT/retransmit thật. Kiểm giao diện bằng trình duyệt trên máy bạn |
| B6 | Hai test B từng skip đã có nội dung và chạy; loss ở cả hai chặng; crash, file dở, reconnect; tải 20 client có barrier và assertion | RAM file lớn, HoL latency và LAN hai máy vẫn cần kiểm sau tích hợp A |
| B7 | README, đặc tả app, sơ đồ chat/file, checklist, báo cáo PDF 5 trang và log kiểm thử | Ghép phần transport/security/CUBIC cuối của A; thêm kết quả LAN; review bản nộp sạch |

Không sửa file trong `transport/` hoặc hằng số wire của A trong lần cập nhật này. Hai test skip còn lại là các placeholder HoL và peer disconnect của A; test crash ở tầng app của B không thay thế hai test đó.

## 2. Cách sử dụng bản cập nhật

Nếu muốn chạy thử độc lập, giải nén gói ZIP và làm theo README trong đó. Gói chứa toàn bộ source dự án đã cập nhật, không chứa thư mục Git, cache hay code mẫu của giảng viên.

Nếu muốn áp dụng vào repo đang làm cùng A, dùng bản vá. Từ thư mục repo của bạn, tạo nhánh riêng trước:

```bash
git switch -c feature/b-application-review
git apply --check /duong/dan/B_application_changes.patch
git apply /duong/dan/B_application_changes.patch
python -m pytest -q
```

Thay `/duong/dan/` bằng đường dẫn file thật. Nếu `--check` báo xung đột, không chép đè hàng loạt; đối chiếu từng file với thay đổi của A. Bản vá dựa trên main revision `4b67076`. Source ở nhánh cục bộ của phiên hỗ trợ chưa được push lên GitHub.

Mật khẩu demo vẫn là `alice123`, `bob123`... tương ứng tên tài khoản + `123`. File users.json đã chuyển sang PBKDF2; file người dùng cũ dùng SHA-256 trần không tương thích. Không đưa tài khoản thật hoặc dữ liệu riêng tư vào các file demo/log.

## 3. Chạy và tự kiểm nhanh

Cài dependency và chạy ba terminal theo README:

```bash
python -m pip install -r requirements.txt
```

Terminal 1:

```bash
python -m server.main --host 127.0.0.1 --port 9000
```

Terminal 2 và 3, mỗi lệnh trong một terminal riêng:

```bash
python -m client.main --server 127.0.0.1:9000 --ui-port 8080
python -m client.main --server 127.0.0.1:9000 --ui-port 8081
```

Mở `http://127.0.0.1:8080` với alice và `http://127.0.0.1:8081` với bob. Mỗi daemon chỉ dùng một tab SSE. Thử lần lượt chat chung, chat riêng, từ chối file, nhận file rỗng, nhận file bình thường, ngắt một client rồi kết nối lại. File nhận nằm trong `downloads/` tính theo thư mục chạy daemon.

Progress hiện tại của transport v1 là **đã xếp vào hàng đợi**. Nó chưa có nghĩa là server đã ACK hoặc người nhận đã lưu file. Chỉ dòng **hoàn tất, hash hợp lệ** từ FILE_DONE mới là xác nhận đầu cuối.

Chạy test:

```bash
python -m pytest -q
```

Kiểm tải với server đang mở:

```bash
python -m tests.load_test --server 127.0.0.1:9000 --clients 20
```

Kỳ vọng đúng 380 broadcast, 0 duplicate; lỗi/timeout làm chương trình trả exit code khác 0. Số memory trong load test chỉ là bộ nhớ Python được trace của tiến trình tạo tải, không phải RAM server và không chứng minh file lớn có bộ nhớ giới hạn.

## 4. Căn cứ và chỗ khác nhau giữa các tài liệu

- Đề Assignment 1 DOCX: Step 2 yêu cầu TCP handshake; Step 5 định tuyến qua server; Step 6 truyền file UDP; Step 9 HTTP UI và JavaScript phía client. Đây là căn cứ rubric 10 bước.
- PDF CO3094 trang 7–11 mô tả thêm raw TCP, xác thực HTTP và P2P; trang 12 dẫn yêu cầu/chấm điểm về Assignment 1. Nếu giảng viên yêu cầu cả các phần bổ sung này, nhóm cần xác nhận lại phạm vi trước khi tuyên bố đáp ứng đầy đủ.
- Repo mẫu `temp2/CO3094_streaprous/daemon/backend.py` có nhánh `asyncio.start_server`, standard socket và raw TCP. Mã mẫu còn TODO ở HTTP parsing, route hook và gửi response. Bản cập nhật không sao chép các file này vào dự án.
- Phân công chính thức: [WORKPLAN](https://github.com/SPKILLER2301/Distributed-Streaming-based-Client-Server/blob/main/WORKPLAN.md). Nguồn code mẫu: [temp2](https://github.com/SPKILLER2301/temp2).

Đối chiếu chi tiết: `docs/AUDIT_B.md`; log thực chạy: `docs/evidence/pytest-B-final.txt`. Báo cáo `docs/report.pdf` là bản bàn giao ứng dụng, chưa phải PDF nghiệm thu cuối cùng. Kiểm bằng trình duyệt chưa hoàn tất do Chromium không khởi động được trong môi trường hỗ trợ; các API HTTP/upload/SSE đã được test tự động.
