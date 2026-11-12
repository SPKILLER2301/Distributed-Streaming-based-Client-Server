# A2 — TCP bootstrap và bàn giao B1

Ngày: 10/11/2026 (Asia/Saigon). Theo WORKPLAN A2 / pipeline §7.

Đã triển khai `transport/bootstrap.py` và `tests/test_bootstrap.py`. Trước A2 client chỉ có UDP handshake; hiện có module TCP discovery độc lập, trả cổng UDP bằng ACK ứng dụng. Server/client production vẫn chờ B1 tích hợp theo phân công. Chưa đánh dấu toàn bộ Step 2 đã nghiệm thu.

## API sử dụng

```python
from transport.bootstrap import BootstrapServer, BootstrapError, bootstrap_connect

bootstrap = BootstrapServer("0.0.0.0", tcp_port=9000, udp_port=9000)
await bootstrap.start()
print(bootstrap.local_addr)
# ... server chạy ...
await bootstrap.stop()

udp_addr = await bootstrap_connect(("127.0.0.1", 9000))
# Sau đó: await endpoint.connect(udp_addr), rồi AUTH.
```

Version mặc định lấy từ `common.constants.PROTOCOL_VERSION` hiện là **1**. Không tự chuyển sang v2. API hỗ trợ timeout keyword (mặc định 3s). Chi tiết framing/lỗi/lifecycle: [protocol §8](protocol.md#8-tcp-bootstrap-a2--đã-có-module-chờ-b1-tích-hợp).

## Việc B1 cần nối

1. Server: bind UDP trước, lấy cổng thật từ `endpoint.local_addr[1]`; mở BootstrapServer trên cổng TCP đã cấu hình và quảng bá cổng UDP đó. TCP/UDP có thể dùng cùng số cổng.
2. Đưa start/stop vào `try/finally`: nếu TCP bind thất bại, dọn UDP; lúc dừng đóng bootstrap và Endpoint. Không chỉ khởi động listener trong script demo.
3. Client: giữ địa chỉ TCP cấu hình gốc để reconnect; gọi bootstrap_connect trước Endpoint.connect, dùng tuple UDP trả về. Không ghi đè địa chỉ bootstrap gốc bằng cổng UDP trả về.
4. Khi bootstrap lỗi, báo trạng thái mạng qua lifecycle/UI, không tiếp tục login bằng UDP ngầm. Reconnect chạy lại TCP → UDP → AUTH. B4 quản lý backoff/cancel và UI vẫn mở khi server offline.
5. CLI/README thể hiện cổng TCP/UDP; nếu chọn cùng 9000 thì firewall LAN cần cả hai protocol. Kiểm shutdown và login/chat qua đường CLI thật.

Ví dụ phần lifecycle server để B tham chiếu (chưa tự động tích hợp vào CollabServer):

```python
bootstrap = None
try:
    await endpoint.start((host, port))
    udp_port = endpoint.local_addr[1]
    bootstrap = BootstrapServer(host, udp_port, udp_port)
    await bootstrap.start()
    await run_application()
finally:
    if bootstrap is not None:
        await bootstrap.stop()
    await endpoint.stop()
```

Nếu CLI chọn TCP port riêng, truyền riêng tcp_port thay vì udp_port trong ví dụ. Bootstrap TCP chưa xác thực danh tính và không cấp session/token; giới hạn MITM của UDP handshake vẫn còn như A1 đã ghi.

## Kiểm thử và bằng chứng

Lệnh focused:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_bootstrap.py -q -p no:cacheprovider
```

Kết quả cuối: **37 passed in 9.59s**, exit 0. [Raw output](evidence/A2-bootstrap-tests.txt).

Các hành vi đã kiểm:

- ACK ứng dụng thực, 20 TCP bootstrap client đồng thời (không phải load test 20 app client).
- Request và response chia nhiều lần ghi; không parse JSON từ một read tùy ý.
- Frame đúng 4096 byte được chấp nhận; quá trần, không newline, JSON/UTF-8 hỏng, sai type/version/missing fields bị từ chối.
- Client từ chối udp_port sai kiểu/range và dùng IP peer thật, bỏ qua host tùy ý trong response.
- Timeout giai đoạn connect (mô phỏng connect không hoàn tất), timeout response, cancellation và EOF; quan sát peer thấy socket đóng.
- Server timeout request, stop khi còn request dở không phải chờ timeout 10s; listener đóng, task/writer dọn; stop lặp/start lại và connection refused.
- Test harness dùng CollabServer/CollabClient thật: TCP và UDP được OS cấp cổng độc lập, bootstrap phải trả đúng cổng UDP thực tế; hai client bootstrap → UDP → login → gửi/nhận DM với assertion nội dung. Mọi resource được dọn trong finally.

Môi trường: Python 3.14.4, Windows 11 build 26200, revision nền `55406b3ad150956ba548c8a416080347872e0b4a` cộng thay đổi A2 chưa commit. Dùng pytest `-p no:cacheprovider` để tránh lỗi cache đã ghi ở A1.

Lệnh toàn suite: `.\.venv\Scripts\python.exe -m pytest -q -rs -p no:cacheprovider`. Kết quả **1 failed, 58 passed, 4 skipped in 9.03s**, exit 1. [Raw output](evidence/A2-full-suite.txt). Fail vẫn là `test_login_broadcast_private_end_to_end`, assertion chat broadcast dòng 60 đã ghi trong baseline A1; test đó không gọi bootstrap. Chưa sửa test app trong A2. B6 cần điều tra predicate chờ events.qsize thay vì chờ đúng event chat. Bốn skip vẫn như A1, chưa bỏ skip.

`git diff --check` đạt. Log stdout được lưu UTF-8; không thêm dependency.

Không có số đo LAN hoặc test qua browser/CLI production trong A2. Handoff B1 còn lại là tích hợp thật, không phải bổ sung module bootstrap.

## Sửa test khi Windows từ chối bind TCP

Traceback người dùng cung cấp: 36 passed, 1 failed; test integration bị WinError 10013 khi bind TCP `127.0.0.1:49665`. Bản test đầu lấy cổng UDP ngẫu nhiên rồi buộc TCP dùng cùng số. Chưa xác định nguyên nhân Windows từ chối cổng đó; kiểm tra excludedportrange hiện tại không thấy 49665 trong dải TCP loại trừ.

Test đã đổi sang tcp_port=0 để OS cấp cổng TCP độc lập và assert ACK trả đúng cổng UDP đã bind. Cấu hình production vẫn có thể chọn cùng số cổng TCP/UDP khi cả hai bind được. Nếu máy gặp lỗi quyền thư mục tạm của pytest, chạy với `--basetemp=.\.pytest_a2_tmp`; thư mục này dành riêng cho pytest và được pytest dọn mỗi lượt.

Xác nhận sau sửa: `.\.venv\Scripts\python.exe -m pytest tests/test_bootstrap.py -q -p no:cacheprovider --basetemp=.\.pytest_a2_tmp` → **37 passed in 6.75s**, exit 0. Lượt trong sandbox bị WinError 5 khi pytest dọn thư mục tạm; lượt được cấp quyền chạy ngoài sandbox đạt toàn bộ. Kết quả sau sửa này là bản ghi tool, không thay raw stdout của lượt A2 trước đó.
