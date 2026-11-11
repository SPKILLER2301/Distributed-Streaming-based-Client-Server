# B application sequences

Source of behavior: client/client.py, server/server.py and server/router.py on the B implementation branch.

## Chat routing

```mermaid
sequenceDiagram
    participant S as Sender daemon
    participant R as Server
    participant D as Destination daemon
    participant O as Other daemon
    S->>R: AUTH on stream 0
    R-->>S: AUTH_OK and user list
    S->>R: MSG_PRIVATE on stream 1
    R->>D: Authenticated sender and text
    Note over O: No private message
    S->>R: MSG_BROADCAST on stream 1
    R->>D: Broadcast
    R->>O: Broadcast
```

## File lifecycle

```mermaid
sequenceDiagram
    participant S as Sender daemon
    participant R as Server relay
    participant D as Receiver daemon
    S->>R: FILE_OFFER metadata on stream 0
    R->>D: FILE_OFFER from authenticated sender
    alt Receiver accepts
        D->>R: FILE_ACCEPT
        R->>S: FILE_ACCEPT
        S->>R: FILE_BEGIN on new file stream
        R->>D: FILE_BEGIN on independent file stream
        loop Until declared size is relayed
            S->>R: Binary chunk
            R->>D: Binary chunk
        end
        Note over D: Verify size and SHA-256
        D->>R: FILE_DONE with boolean result
        R->>S: FILE_DONE after relay completed
    else Receiver rejects
        D->>R: FILE_REJECT
        R->>S: FILE_REJECT and cleanup
    end
    Note over S,D: Disconnect aborts the transfer without resume
```

For a zero-byte file, BEGIN completes the relay and receiver verifies the empty-file hash without waiting for a chunk. After a peer disconnect or inactivity deadline, the relay notifies the remaining participant with FILE_ERROR and removes application state. Transport buffer cancellation remains pending A5.
