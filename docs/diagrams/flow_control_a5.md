# Backpressure A5 — wire v2

Đã triển khai ở transport và adapter producer/relay tối thiểu. B3/B5 hoàn thiện file state, upload và UI. [Bàn giao A5](../A5-flow-control.md).

```mermaid
sequenceDiagram
    participant P as Producer client
    participant A as Client Endpoint
    participant S as Server Endpoint và relay
    participant B as Receiver Endpoint
    participant D as Consumer ghi file
    P->>A: await send_message(chunk)
    Note over P,A: Chờ khi queue gửi đầy, enqueue nguyên message
    A->>S: DATA nếu còn cwnd và credit byte/packet
    S-->>A: ACK sau khi giữ DATA trong receive budget
    Note over A: Receipt tăng ACK bytes, không đếm retransmit
    S->>B: await enqueue relay, rồi phát DATA theo credit
    Note over S: Chỉ consume đầu vào sau bounded enqueue chặng sau
    B-->>S: ACK transport
    B->>D: Message theo thứ tự stream
    Note over B,D: Consumer chậm giữ credit, không block handler chat khác
    D->>B: consume(message)
    B-->>S: CREDIT tích lũy byte và packet-slot
    Note over S: Chặng sau tiến triển, queue relay có chỗ
    S-->>A: CREDIT khi consume đầu vào
    A-->>P: Queue có chỗ cho chunk tiếp theo
```

CREDIT bị mất được phục hồi qua probe; update cũ/trùng không cộng credit lần hai. ACK chặng đầu không phải FILE_DONE. File được cấp tối đa connection budget trừ reserve control/chat; scheduler giữ lượt file ngay cả khi chat liên tục.
