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
    private final RestTemplate restTemplate;

    public String handleRead(String key) throws IllegalStateException {
        if (raftConfig.isEnableFollowerReads()) {
            if (!raftNode.isLeader()) {
                Integer readIndex = requestReadIndexFromLeader();
                waitForLogToSync(readIndex);
                return kvStore.get(key); 
            } else {
                return performLeaderRead(key);
            }
        } else {
            if (!raftNodeState.isLeader()) {
                throw new IllegalStateException("Read requests must be routed to the leader when follower reads are disabled");
            }
            return performLeaderRead(key);
        }
    }

    private ReadIndexResponseDTO handleReadIndex(){
        Integer readIndex = raftLogManager.getCommitIndex();
        confirmLeadership();
        ReadIndexResponseDTO response = new ReadIndexResponseDTO(readIndex);
        return response;
    }

    private Integer requestReadIndexFromLeader() {
        String leaderUrl = raftNodeState.getLeaderUrl();
        if (leaderUrl == null) {
            throw new IllegalStateException("Leader unknown. Cannot perform follower read.");
        }
    
        try {
            ResponseEntity<ReadIndexResponseDTO> response = restTemplate.getForEntity(
                leaderUrl + "/raft/rpc/readIndex",
                ReadIndexResponseDTO.class
            );
    
            ReadIndexResponseDTO body = response.getBody();
            if (response.getStatusCode().is2xxSuccessful() && body != null) {
                return body.getReadIndex();
            } else {
                throw new IllegalStateException("Failed to obtain read index from leader.");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error while contacting leader for read index", e);
        }
    }

    private void waitForLogToSync(Integer readIndex) {
        while (raftNodeState.getLastApplied() < readIndex) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while syncing log for read", e);
            }
        }
    }

    private String performLeaderRead(String key) {
        Integer readIndex = raftLogManager.getCommitIndex();
        confirmLeadership();
        waitForLogToSync(readIndex);
        return kvStore.get(key);
    }

    private void confirmLeadership() {
        int currentTerm = raftNodeState.getCurrentTerm();
        int confirmations = 1; 
    
        for (String peerUrl : raftNode.getPeerUrls()) {
            try {
                HeartbeatResponse response = restTemplate.postForObject(
                    peerUrl + "/raft/confirmLeadership", 
                    new confirmLeadershipRequest(raftNodeState.getNodeId(), currentTerm),
                    HeartbeatResponse.class
                );
                if (response != null && response.isSuccess() && response.getTerm() == currentTerm) {
                    confirmations++;
                }
            } catch (Exception ignored) {
                // Node unreachable or failed, ignore
            }
        }
    
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        if (confirmations < majority) {
            throw new IllegalStateException("Leadership not confirmed: quorum not achieved");
        }
    }
}
