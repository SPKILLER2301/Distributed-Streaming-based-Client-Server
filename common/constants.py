"""
Hằng số dùng chung cho server, client và transport.      [Người phụ trách: A+B]

QUY TẮC: file này là "hợp đồng" giữa hai người. Sửa giá trị nào thì báo người kia.
"""

# ---- Phiên bản & địa chỉ mặc định -----------------------------------------
PROTOCOL_VERSION = 2
DEFAULT_SERVER_HOST = "127.0.0.1"
DEFAULT_SERVER_PORT = 9000
DEFAULT_UI_PORT = 8080

# ---- Loại gói (ptype) -------------------------------------------------------
PKT_DATA = 0
PKT_ACK = 1
PKT_HELLO = 2
PKT_HELLO_REPLY = 3
PKT_PING = 4
PKT_PONG = 5
PKT_CLOSE = 6
PKT_CREDIT = 7
PKT_CREDIT_PROBE = 8
PKT_RESET_STREAM = 9
PKT_RESET_ACK = 10
PKT_STOP_STREAM = 11

CONTROL_BUDGET = 16      # authenticated control frames per tick, coalesced by type/stream

# ---- Cờ (flags) -------------------------------------------------------------
FLAG_EOM = 0x01          # gói cuối cùng của một message (End-Of-Message)

# ---- Stream ------------------------------------------------------------------
STREAM_CONTROL = 0       # AUTH, danh sách user, tín hiệu file
STREAM_CHAT = 1          # chat broadcast / private
STREAM_FILE_BASE = 2     # stream >= 2: mỗi file một stream

# ---- Kích thước ---------------------------------------------------------------
HEADER_SIZE = 23         # phải khớp struct.calcsize(HEADER_FORMAT) trong packet.py
MAX_DATAGRAM = 1200      # giữ dưới MTU để tránh phân mảnh IP
GCM_TAG_SIZE = 16
MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE - GCM_TAG_SIZE   # byte dữ liệu tối đa mỗi gói
FILE_CHUNK_SIZE = 8192   # kích thước 1 chunk file (1 message, sẽ được cắt thành nhiều gói)

# ---- Độ tin cậy / cửa sổ ------------------------------------------------------
SACK_BITS = 32           # số bit trong bitmap SACK
MAX_WINDOW = 64          # cửa sổ tối đa (gói). Tăng lên thì cần tăng SO_RCVBUF của socket UDP, kẻo tràn bộ đệm
INITIAL_CWND = 10        # cửa sổ tắc nghẽn khởi đầu (gói)
INITIAL_RTO = 0.5        # giây
MIN_RTO = 0.2
MAX_RTO = 5.0
MAX_RETRIES = 10         # gửi lại quá số lần này -> coi như mất kết nối

# ---- Thời gian ----------------------------------------------------------------
TICK_INTERVAL = 0.02     # chu kỳ vòng lặp timer của transport
HEARTBEAT_INTERVAL = 2.0 # im lặng quá lâu thì gửi PING
PEER_TIMEOUT = 10.0      # không nhận được gì quá lâu -> đóng kết nối
HANDSHAKE_TIMEOUT = 1.5
HANDSHAKE_RETRIES = 4
MAX_CONNECTIONS = 64     # per Endpoint, established transport sessions (A6)
