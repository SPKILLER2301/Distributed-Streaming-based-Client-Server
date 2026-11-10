# Sơ đồ transport — A1

Ngày 09/11/2026. Nguồn hành vi và hợp đồng: [protocol](../protocol.md). A phụ trách kiến trúc/state/handshake/loss; B phụ trách sequence chat/file ứng dụng.

Cập nhật A2, 10/11/2026: module TCP bootstrap và test harness TCP → UDP → login/chat đã chạy; các nhãn bootstrap “dự kiến” trong sơ đồ chỉ phần tích hợp production B1 chưa thực hiện. Xem [bàn giao A2](../A2-bootstrap.md).

## Kiến trúc (TCP bootstrap là phần dự kiến)

```mermaid
flowchart LR
    Browser[Browser] <-->|HTTP và SSE qua TCP hiện có| UI[HTTP UI trong client daemon]
    UI <--> App[Client app]
    App <--> EP[Client Endpoint]
    EP <-->|UDP DATA mã hóa từng chặng| Server[Server Endpoint và router]
    Server <-->|UDP relay| Peer[Peer Endpoint và app]
    App -. TCP bootstrap dự kiến .-> Boot[Server bootstrap listener]
```

Server đọc plaintext để routing/relay; không phải mã hóa E2E.

## State logic hiện tại (không phải enum trong code)

```mermaid
stateDiagram-v2
    [*] --> SocketReady: await start
    SocketReady --> HelloPending: client connect
    HelloPending --> HelloPending: retry HELLO, tối đa 4 lượt
    HelloPending --> Established: HELLO_REPLY hợp lệ
    HelloPending --> SocketReady: hết retry, connect raise
    SocketReady --> Established: server nhận HELLO hợp lệ
    Established --> Closed: local close hoặc peer CLOSE
    Established --> Closed: peer timeout hoặc hết retry DATA
    Closed --> [*]: xóa connection và HELLO cache
```

Endpoint có thể giữ nhiều connection; state trên chỉ mô tả một phiên. Server on_connect xảy ra khi tạo connection, chưa phải login thành công. Close hiện không chờ CLOSE ACK.

## Handshake mục tiêu A2 + B1

```mermaid
sequenceDiagram
    participant C as Client daemon
    participant T as Server TCP bootstrap
    participant U as Server UDP Endpoint
    participant A as Server auth
    Note over C,T: Dự kiến A2/B1, chưa có trong runtime
    C->>T: TCP connect, BOOTSTRAP(version)
    T-->>C: BOOTSTRAP_ACK(version, udp_port)
    Note over C,T: Đọc đủ frame, kiểm tra, đóng TCP
    C->>U: HELLO(client public key)
    U-->>C: HELLO_REPLY(conn_id, server public key)
    Note over C,U: X25519 + HKDF, khóa riêng mỗi chiều
    C->>U: DATA stream 0: AUTH mã hóa
    U->>A: Message AUTH đã giải mã
    A-->>C: AUTH_OK hoặc AUTH_FAIL qua UDP DATA
```

Sai version/oversize/timeout bootstrap phải thất bại rõ ràng, không đi thẳng UDP. UDP HELLO có retry riêng; reconnect do B chạy lại cả bootstrap, UDP và auth.

## Loss/recovery mục tiêu A3/A7

```mermaid
sequenceDiagram
    participant S as Sender
    participant R as Receiver
    S->>R: DATA file seq 0
    S--xR: DATA file seq 1 bị mất
    S->>R: DATA file seq 2, 3, 4
    R-->>S: ack=1, sack=0b111
    S->>R: DATA chat trên stream 1
    Note over R: Giao chat trong khi file còn thiếu seq 1
    Note over S: Ba seq khác nhau làm bằng chứng loss
    S->>R: Retransmit file seq 1, nguyên wire bytes
    R-->>S: ack=5
    Note over R: Ghép và giao file message theo EOM
```

Test HoL phải cố ý giữ lỗ hổng tới khi chat được giao; hình trên không phải bằng chứng test đã chạy. ACK trùng không phải bằng chứng loss mới. Tail loss không có ba packet sau thì dùng RTO. Không bảo đảm chat latency không đổi vì cwnd vẫn dùng chung.
