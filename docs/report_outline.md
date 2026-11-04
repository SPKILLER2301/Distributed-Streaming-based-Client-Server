# Dàn ý báo cáo PDF  [A+B]

1. **System Architecture** – sơ đồ client ↔ server, các tầng (UI / app / transport / UDP). [B]  → `diagrams/architecture.png`
2. **Protocol Specification** – header, loại gói, message JSON, mã hóa, cửa sổ/ACK/RTO/congestion. [A]  (lấy từ `protocol.md`)
3. **Sequence Diagram** – vòng đời một tin nhắn [B] (`seq_message.png`) và một lần truyền file [A] (`seq_file.png`).
4. **Challenges & Solutions** – đúng 1 trang: HoL blocking, mất gói/đảo thứ tự, deadlock, phân mảnh... [A+B]
5. (Tùy chọn) Kết quả kiểm thử với mất gói 5–20 %, số client tối đa đã thử.

Xuất ra PDF rồi đặt tên `docs/report.pdf`.
