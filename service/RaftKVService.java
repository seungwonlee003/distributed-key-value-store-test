package com.example.raft.service;

import com.example.raft.config.RaftConfig;
import com.example.raft.storage.KVStore;
import com.example.raft.RaftNode;
import com.example.raft.RaftLogManager;
import com.example.raft.node.RaftNodeState;
import com.example.raft.dto.ConfirmLeadershipRequest;
import com.example.raft.dto.HeartbeatResponse;
import com.example.raft.dto.ReadIndexResponseDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

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
            if (!raftNodeState.isLeader()) {
                int readIndex = requestReadIndexFromLeader();
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

    private int requestReadIndexFromLeader() {
        String leaderUrl = raftNodeState.getCurrentLeaderUrl();
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

    public ReadIndexResponseDTO getSafeReadIndex() {
        if (!raftNodeState.isLeader()) {
            throw new IllegalStateException("Not leader. Cannot serve read index request.");
        }
        confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        return new ReadIndexResponseDTO(readIndex);
    }
    
    private void waitForLogToSync(int readIndex) {
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
        confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        waitForLogToSync(readIndex);
        return kvStore.get(key);
    }

    private void confirmLeadership() {
        if (!raftNodeState.isLeader()) {
            throw new IllegalStateException("Not leader.");
        }
        
        int currentTerm = raftNodeState.getCurrentTerm();
        int confirmations = 1; // count self

        for (String peerUrl : raftConfig.getPeerUrlList()) {
            try {
                HeartbeatResponse response = restTemplate.postForObject(
                    peerUrl + "/raft/confirmLeadership", 
                    new ConfirmLeadershipRequest(raftNodeState.getNodeId(), currentTerm),
                    HeartbeatResponse.class
                );
                if (response != null && response.isSuccess() && response.getTerm() == currentTerm) {
                    confirmations++;
                } else if(response.getTerm() > currentTerm){
                    stateManager.becomeFollower(response.getTerm());
                    throw new IllegalStateException("Split brain scenario");
                } 
            } catch (Exception ignored) {
                // Ignore unreachable peers.
            }
        }
        int majority = (raftConfig.getPeerUrlMap().size()) / 2 + 1;
        if (confirmations < majority) {
            throw new IllegalStateException("Leadership not confirmed: quorum not achieved");
        }
    }

    public HeartbeatResponse handleConfirmLeadership(ConfirmLeadershipRequest request) {
        if (request.getTerm() > raftNodeState.getCurrentTerm()) {
            stateManager.becomeFollower(request.getTerm());
            return new HeartbeatResponse(false, request.getTerm());
        }
        if (raftNodeState.getCurrentRole() != Role.FOLLOWER) {
            return new HeartbeatResponse(false, raftNodeState.getCurrentTerm());
        }
        boolean success = request.getTerm() == raftNodeState.getCurrentTerm();
        if (raftNodeState.getCurrentLeader() != null &&
            !raftNodeState.getCurrentLeader().equals(request.getNodeId())) {
            success = false;
        }
        return new HeartbeatResponse(success, raftNodeState.getCurrentTerm());
    }
}
