package com.example.raft.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class InMemoryKVStore implements KVStore {
    private final Map<String, String> store;

    public InMemoryKVStore() {
        this.store = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, String value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        if (value == null) throw new IllegalArgumentException("Value cannot be null");
        store.put(key, value);
    }

    @Override
    public void remove(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        if (!store.containsKey(key)) {
            throw new IllegalStateException("Key '" + key + "' does not exist for removal");
        }
        store.remove(key);
    }

    @Override
    public String get(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return store.get(key);
    }

    @Override
    public boolean containsKey(String key) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        return store.containsKey(key);
    }

    public Map<String, String> getSnapshot() {
        return new ConcurrentHashMap<>(store);
    }

    public void clear() {
        store.clear();
    }
}
