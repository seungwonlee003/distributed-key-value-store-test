package com.example.raft.storage;

public interface KVStore {
    void put(String key, String value);
    void remove(String key);
    String get(String key);
    boolean containsKey(String key);
    Long getLastSequenceNumber(String clientId);
    void setLastSequenceNumber(String clientId, Long sequenceNumber);
}
