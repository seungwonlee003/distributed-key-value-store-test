package com.example.raft.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Component
@Getter
@Setter
public class InMemoryKVStore implements KVStore {
    private final Map<String, String> store;
    private final Map<String, Integer> clientStore;

    public InMemoryKVStore() {
        this.store = new ConcurrentHashMap<>();
        this.clientStore = new ConcurrentHashMap<>();
    }

    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    @Override
    public void remove(String key) {
        if (!store.containsKey(key)) {
            throw new IllegalStateException("Key '" + key + "' does not exist for removal");
        }
        store.remove(key);
    }

    @Override
    public String get(String key) {
        return store.get(key);
    }

    @Override
    public boolean containsKey(String key) {
        return store.containsKey(key);
    }
}
