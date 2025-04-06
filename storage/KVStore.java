package com.example.raft.storage;

public interface KVStore {
    void put(String key, String value);
    void remove(String key);
    String get(String key);
    boolean containsKey(String key);
    Long getLastRequestId(String clientId);
    void setLastRequestId(String clientId, Long requestId);
}
