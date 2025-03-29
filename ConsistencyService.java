package com.example.raft.service;

import com.example.raft.model.ConsistencyLevel;
import com.example.raft.storage.KVStore;
import com.example.raft.RaftNode;
import com.example.raft.RaftLogManager;

@Service
@RequiredArgsConstructor
public class ConsistencyService {
    private final RaftNode raftNode;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;

    public String read(String key, ConsistencyLevel level) throws ConsistencyException {
        switch (level) {
            case DEFAULT:
                return kvStore.get(key);
            
            case LINEARIZABLE:  
                // confirm its leadership
                // ensures lastApplied == commitIndex
                // read the value from KVStore

                raftLogManager.replicateLogToFollowers(null);
                if(raftNode.getRole() != Role.LEADER){
                    throw new IllegalStateException("Leadership lost");
                }
                return kvStore.get(key))
            
            case EVENTUAL:
                return kvStore.get(key);

            default:
                throw new IllegalArgumentException("Unknown consistency level: " + level);
        }
    }
}
