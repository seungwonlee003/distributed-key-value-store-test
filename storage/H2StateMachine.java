package com.example.raft.storage;

import com.example.raft.log.LogEntry;
import org.springframework.stereotype.Component;

@Component
public class RaftStateMachine implements StateMachine {
    private final KVStore kvStore;

    public RaftStateMachine(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void apply(LogEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Log entry cannot be null");
        }

        // Client-side de-duplication logic
        String clientId = entry.getClientId();
        long sequenceNumber = entry.getSequenceNumber();
        Long lastRequestId = kvStore.getLastRequestId(clientId);
        if (lastRequestId != null && sequenceNumber <= lastRequestId) {
            return; // Skip duplicate request
        }

        // Apply state machine operations
        switch (entry.getOperation()) {
            case INSERT:
                if (kvStore.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Key '" + entry.getKey() + "' already exists for INSERT");
                }
                kvStore.put(entry.getKey(), entry.getValue());
                break;

            case UPDATE:
                if (!kvStore.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Key '" + entry.getKey() + "' does not exist for UPDATE");
                }
                kvStore.put(entry.getKey(), entry.getValue());
                break;

            case DELETE:
                kvStore.remove(entry.getKey());
                break;

            default:
                throw new IllegalStateException("Unknown operation: " + entry.getOperation());
        }

        // Update client sequence number
        kvStore.setLastRequestId(clientId, sequenceNumber);
    }
}
