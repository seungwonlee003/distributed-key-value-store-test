package com.example.raft.service;

import com.example.raft.model.ConsistencyLevel;
import com.example.raft.storage.KVStore;
import com.example.raft.RaftNode;
import com.example.raft.RaftLogManager;

public class ConsistencyService {
    private final RaftNode raftNode;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;

    public ConsistencyService(RaftNode raftNode, RaftLogManager raftLogManager, KVStore kvStore) {
        this.raftNode = raftNode;
        this.raftLogManager = raftLogManager;
        this.kvStore = kvStore;
    }

    public String read(String key, ConsistencyLevel level) throws ConsistencyException {
        switch (level) {
            case DEFAULT:
                return kvStore.get(key);
            
            case LINEARIZABLE:  
                return;
            
            case EVENTUAL:
                return kvStore.get(key);

            default:
                throw new IllegalArgumentException("Unknown consistency level: " + level);
        }
    }
}
