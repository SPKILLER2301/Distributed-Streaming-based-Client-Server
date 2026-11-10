# A1 — Audit transport và hợp đồng bàn giao

Ngày: 09/11/2026 (Asia/Saigon). Căn cứ: [WORKPLAN A1](../WORKPLAN.md), [pipeline §5–6](../pipeline.md), mã nguồn tại revision `93812e930cc0cc8dc1987a4f6d47c0f4d7bac220`.

**Kết quả:** hoàn thành audit và bản thiết kế A1 để triển khai/review. Không thay đổi hành vi runtime trong task này. [Protocol](protocol.md) mục 1–4 là hiện tại, mục 5–8 là hợp đồng triển khai mới. Chưa có xác nhận của B; chưa đánh dấu J1 (thống nhất hai người) hoặc A2–A8 hoàn thành.

## 1. Phạm vi và bằng chứng

Đã đọc `common/constants.py`, `common/packet.py`, toàn bộ module transport, test packet/reliability và simulator; đối chiếu call site trong client, server, router và test app. Thư mục hiện **có Git**, khác ghi chú cũ trong WORKPLAN/pipeline. Git status sạch trước khi làm A1.

Kết quả thật: Python 3.14.4, Windows, toàn suite **21 passed, 1 failed, 4 skipped**, transport/packet **11 passed, 2 skipped**. Xem [bằng chứng và lệnh](evidence/A1-baseline.md). Không lấy “22 pass” cũ làm baseline. Không chạy browser/LAN/load 20 client trong A1.

## 2. Audit có/thiếu/lỗi và kế hoạch kiểm chứng

Các finding dưới đây là suy ra từ đọc code trừ những kết quả test ghi riêng; chưa có exploit/reproduction cho mọi finding.

| ID | Điểm mã nguồn | Đã có | Thiếu/lỗi và ảnh hưởng | Task / kiểm chứng cần thêm |
|---|---|---|---|---|
| T01 | `common/packet.py:decode` | Header 23 byte, version, length check | Chưa reject unknown type/flags, datagram quá 1200, field không hợp lệ theo type | A6: malformed/type/oversize không đổi state hoặc làm ngừng client tốt |
| T02 | `stream.py:enqueue`, `RecvStream.on_packet` | Fragment/EOM, FIFO trong stream, duplicate suppression | pending và fragments không có trần byte/message; message không EOM giữ RAM vô hạn | A5: trần byte/message, receiver chậm, thiếu EOM, empty message flood |
| T03 | `SendStream.on_ack` | Pop unacked theo cumulative ACK/SACK; duplicate không cộng ACK đã pop | ACK chưa xác thực, không kiểm ack vượt highest-sent; có thể giải phóng dữ liệu chưa nhận thật | A3/A6: forged/out-of-range ACK không đổi inflight/cwnd/receipt |
| T04 | `RecvStream.ack_info`, `Connection._pump` | Reorder window 64; SACK 32 bit | SACK không phủ cả window; sender chỉ đếm inflight, không ràng buộc seq mới với cumulative ACK | A3: giữ một lỗ hổng rồi gửi nhiều hơn 64 packet; không đẩy seq vượt receive window |
| T05 | `Connection._on_ack`, `on_tick` | RTT Karn; RTO/backoff chung; giới hạn retry | Chưa fast retransmit; mọi timeout cùng gửi tạo burst; chưa recovery boundary/pacing | A3: distinct SACK, tail/ACK/retransmit loss, retry exhaustion; đếm số lần giảm cửa sổ |
| T06 | `congestion.py:CongestionController` | AIMD, slow start, cwnd 10..64 trước timeout | Chưa CUBIC; comment “tùy chọn/đủ điểm” không theo WORKPLAN | A4: clock giả, ACK/loss/timeout, cấu hình demo và nguồn đặc tả |
| T07 | `Connection._pump` | Duyệt mỗi stream một packet/lượt khi còn cwnd | Mỗi pump lại bắt đầu stream đầu, không giữ con trỏ; stream đầu có thể chiếm chỗ khi chỉ còn một slot | A5/A7: nhiều file + chat liên tục, file nào cũng tiến triển |
| T08 | `Endpoint.send`, callbacks | API sync, bool enqueue, callback dữ liệu hoàn chỉnh | Chưa await/backpressure/ACK receipt/consume/cancel/stats công khai; callback lỗi sau ACK không retry app | A5: close trong khi send/ACK/consume chờ; B đổi callback/relay |
| T09 | `crypto.py`, `_transmit` | X25519/HKDF, key theo chiều, DATA AES-GCM/AAD | Mã hóa lại khi retransmit thay vì cache; comment bỏ qua bất biến AAD; control chưa xác thực | A6: bytes retransmit bất biến, tamper cả header/tag, control replay/counter |
| T10 | `Connection.handle_packet` | Theo dõi last_recv và peer timeout | Cập nhật last_recv trước DATA decrypt, cả unknown type; rác có thể kéo dài phiên | A6: gói invalid/tag sai không gia hạn timeout |
| T11 | `open_stream`, `enqueue` | Stream tăng dần, seq riêng từng chiều | Không chặn overflow uint16/uint32; không trần stream/connection; reuse sau wrap gây rủi ro nonce | A6: sát giới hạn field, đóng có lý do trước wrap |
| T12 | `Endpoint._handle_hello` | Cache HELLO trùng, xóa cache khi conn đóng | Chưa giới hạn cấp connection; HELLO public key mới đóng connection cũ trước derive key thành công | A6: HELLO lỗi từ cùng addr không phá phiên tốt; capacity/handshake lifecycle |
| T13 | `Endpoint.connect/stop`, `_finish` | Retry handshake, disconnect callback; xóa conn/cache | Một `_pending` nên connect đồng thời ghi đè; stop không await tick và không kết thúc handshake future ngay; chưa dọn rõ mọi queue/waiter | A6: stop khi handshake/gửi đang chờ, repeated close, task/handle không tồn đọng |
| T14 | `Endpoint.sendto/error_received` | Catch OSError, log lỗi UDP; datagram dispatch có exception guard | Không thể kết luận WinError 10054 mọi tình huống đã xử lý; log lỗi chưa giới hạn tần suất | A6/A7: fault injection + LAN Windows; kiểm server còn phục vụ |
| T15 | `tests/test_reliability.py` | Test handshake hai chiều, fragmentation, FIFO, loss 10%, jitter/duplicate | HoL/disconnect chỉ stub skip; test loss/reorder thiếu stop rõ ràng, các test chưa cleanup khi assert lỗi | A7: drop đích danh và finally cleanup; không chỉ bỏ skip |
| T16 | `transport/` và startup client/server | UDP HELLO có gửi lại | Chưa bootstrap TCP client–server | A2 module/test; B1 tích hợp vào startup/reconnect thật |
| T17 | `server/server.py:_handle_auth` / client / router | App gọi Endpoint, truyền file được trong test hiện tại | Server còn đọc connections để lấy addr; relay sync enqueue không bounded; progress đếm enqueue | A5 API get_peer_addr/receipt; B3 producer/relay dùng await/consume |

## 3. Quyết định thiết kế A1

Chi tiết có tính hợp đồng ở [protocol mục 5–8](protocol.md); bảng này là bản đồ triển khai, không thay thế spec.

| Quyết định | Phương án chọn | Lý do / ảnh hưởng |
|---|---|---|
| Đơn vị | cwnd/window là packet, queue/credit/receipt là plaintext byte, timer giây monotonic | Không trộn byte với packet; header/tag không tăng progress file |
| Message/sequence | Giữ seq per-stream và EOM; không đổi thành QUIC byte offset | Giảm phạm vi thay đổi; phải giữ cửa sổ sequence ngoài cwnd |
| API gửi | async send_message → receipt; wait_acked riêng | Producer có backpressure nhưng không phải chờ mỗi chunk ACK tuần tự |
| API nhận | handler async theo stream + consume token idempotent | Giữ FIFO trong stream, không để file chậm chặn handler chat |
| Ownership | Consumer chỉ trả credit khi đã xử lý hoặc chuyển ownership sang queue giới hạn | Backpressure lan từ receiver qua relay tới sender |
| ACK progress | receipt từng message, ACK mới một lần | B loại receipt BEGIN, chỉ cộng chunk; DONE vẫn do receiver xác minh |
| Cancel | Theo chiều gửi cục bộ, reset tin cậy/final_bytes/tombstone | Stream cùng số ở hai chiều không bị hủy nhầm; credit không rò khi bỏ dữ liệu |
| Wire version | Giữ header nhưng lên v2 khi thêm authenticated control/credit | V1/v2 không được hiểu sai dữ liệu nhau; A1 chưa đổi runtime version |
| Crypto | Domain DATA/control riêng, counter control 64 bit và replay window; cache DATA | Không trùng nonce giữa các loại gói; retry có quy tắc rõ |
| Giới hạn | Fixed v2 profile, bảng protocol §7 | Có điểm xuất phát cụ thể để B viết bounded producer/relay và test |
| Fairness | Weighted 4 control/chat : 1 file, con trỏ file qua pump | Ưu tiên độ trễ nhưng file vẫn tiến triển nếu đủ cwnd/credit |
| Bootstrap | JSON newline ≤4096 byte, timeout 3s, start/stop/connect riêng | A2 làm module; B1 gọi trong vòng đời thật |

Profile v2 giới hạn message file 16 KiB, control/chat 64 KiB. Vì vậy test hiện gửi **một message 512 KB/200 KB** sẽ không hợp lệ sau A5. Khi triển khai, đổi test fragmentation sang message hợp lệ và thêm test MessageTooLarge; vẫn giữ kiểm thử tổng tải lớn bằng nhiều message. Không nới giới hạn chỉ để giữ test cũ.

## 4. State và phần việc B cần nối

[Sơ đồ](diagrams/transport_a1.md) phân biệt state logic hiện có, bootstrap dự kiến và loss/recovery. Không thêm enum runtime trong A1.

1. B1 dùng module bootstrap trước Endpoint.connect; login chỉ sau UDP handshake.
2. B3 một producer có giới hạn/task mỗi file; await send_message cho BEGIN và chunk cùng stream; tính ACK từ receipt chunk.
3. B3 chuyển receive sang handler async; await relay enqueue rồi consume. Không create_task cho từng chunk rồi consume ngay.
4. B4 đóng/hủy consumer và file handle theo reason; reconnect tạo phiên mới, không resume ngầm.
5. B5 lấy snapshot stats/public peer accessor; không đọc internals transport trong UI.
6. B6 kiểm file relay cả hai chặng, cleanup app, 20 client; A cung cấp loss hook/test transport.

Các giao diện trên đã được A xác định thành tài liệu có thể review; **chưa ghi nhận B review/tích hợp**. Khi B có phản hồi, sửa spec trước code chéo. File app/message không sửa trong A1. Message schema hiện tại ở protocol §2 giữ cho B1 rà soát.

## 5. Kết quả nghiệm thu A1 và bước kế tiếp

- [x] Audit transport, có/thiếu/lỗi và test cần chứng minh.
- [x] Ghi protocol hiện tại, ACK/SACK, timer, unit và state thực tế.
- [x] Tách API hiện có và hợp đồng API dự kiến.
- [x] Chọn giới hạn, ownership, close/cancel/error/stats.
- [x] Thiết kế version, nonce/control/retry trước A3/A5.
- [x] Lưu baseline thực tế và sơ đồ có thể đọc/review.
- [ ] J1: B review hợp đồng và cùng xác nhận môi trường (việc chung, chưa hoàn thành).

Tiếp theo làm A2 dựa trên protocol §8; A3/A4/A5/A6 triển khai theo hợp đồng. Cập nhật spec cùng code khi có thay đổi; không đánh dấu tính năng đã hoàn thành chỉ vì đã thiết kế.
