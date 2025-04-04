package com.example.raft.storage;

import com.example.raft.log.LogEntry;

@Component
public class InMemoryStateMachine implements StateMachine {
    private final KVStore kvStore;

    public InMemoryStateMachine(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public void apply(LogEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Log entry cannot be null");
        }

        // deduplication logic
        String clientId = entry.getClientId();
        long sequenceNumber = entry.getSequenceNumber();

        Map<String, Long> clientStore = kvStore.getClientStore();
        Long lastRequestId = clientStore.get(clientId);
        if (lastRequestId != null && requestId <= lastRequestId) {
            return;
        }

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
                kvStore.remove(entry.getKey()); // Exception handled in KVStore
                break;

            default:
                throw new IllegalStateException("Unknown operation: " + entry.getOperation());
        }

        clientStore.put(clientId, requestId);
    }
}
