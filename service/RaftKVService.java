package com.example.raft.service;

import com.example.raft.config.RaftConfig;
import com.example.raft.storage.KVStore;
import com.example.raft.RaftNode;
import com.example.raft.RaftLogManager;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RaftKVService { 
    private final RaftNode raftNode;
    private final RaftNodeState raftNodeState;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;
    private final RaftConfig raftConfig;

    public String handleRead(String key) throws IllegalStateException {
        if (raftConfig.isEnableFollowerReads()) {
            if (!raftNode.isLeader()) {
                long readIndex = requestReadIndexFromLeader();
                waitForLogToSync(readIndex);
                return kvStore.get(key); 
            } else {
                return performLeaderRead(key);
            }
        } else {
            if (!raftNode.isLeader()) {
                throw new IllegalStateException("Read requests must be routed to the leader when follower reads are disabled");
            }
            return performLeaderRead(key);
        }
    }

    private long requestReadIndexFromLeader() {
        
    }

    private void waitForLogToSync(long readIndex) {
        while (raftNodeState.getLastApplied() < readIndex) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while syncing log for read", e);
            }
        }
    }

    private String performLeaderRead(String key) {
        long readIndex = raftLogManager.getCommitIndex(); 
        confirmLeadership();
        return kvStore.get(key);
    }

    private void confirmLeadership(){
        
    }
}
