package com.example.raft.storage;

import com.example.raft.log.LogEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStateMachine implements StateMachine {
    private final Map<String, String> state;

    public InMemoryStateMachine() {
        this.state = new ConcurrentHashMap<>();
    }

    @Override
    public void apply(LogEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Log entry cannot be null");
        }

        switch (entry.getOperation()) {
            case INSERT:
                if (state.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Key '" + entry.getKey() + "' already exists for INSERT");
                }
                state.put(entry.getKey(), entry.getValue());
                break;

            case UPDATE:
                if (!state.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Key '" + entry.getKey() + "' does not exist for UPDATE");
                }
                state.put(entry.getKey(), entry.getValue());
                break;

            case DELETE:
                if (!state.containsKey(entry.getKey())) {
                    throw new IllegalStateException("Key '" + entry.getKey() + "' does not exist for DELETE");
                }
                state.remove(entry.getKey());
                break;

            default:
                throw new IllegalStateException("Unknown operation: " + entry.getOperation());
        }
    }

    public String get(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return state.get(key);
    }

    public boolean containsKey(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return state.containsKey(key);
    }

    public int size() {
        return state.size();
    }

    public void clear() {
        state.clear();
    }

    public Map<String, String> getStateSnapshot() {
        return new ConcurrentHashMap<>(state);
    }

    public void applyBatch(List<LogEntry> entries) {
        if (entries == null) throw new IllegalArgumentException("Entries list cannot be null");
        for (LogEntry entry : entries) {
            apply(entry);
        }
    }
}
