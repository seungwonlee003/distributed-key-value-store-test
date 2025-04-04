package com.example.raft.log;

@Getter
@Setter
@RequiredArgsConstructor
public class LogEntry {
    private final int term;          // Raft term for this entry
    private final String key;        // Key for the operation
    private final String value;      // Value for INSERT/UPDATE (null for DELETE)
    private final Operation operation; // Enum for operation type

    // unique client id
    private final String clientUUID;
    // monotonomically increasing per request (unless same request)
    private final int sequenceNumber;

    public enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }
}
