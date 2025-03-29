package com.example.raft;

import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;

@Configuration
public class RaftInitializer {

    private final RaftNode raftNode;

    public RaftInitializer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @PostConstruct
    public void init() {
        // Force the node to start as a follower.
        RaftNodeState state = raftNode.getState();
        state.setRole(Role.FOLLOWER);
        state.setCurrentTerm(0);
        state.setVotedFor(null);
        System.out.println("Node " + state.getNodeId() + " starting as FOLLOWER");

        // Ensure peer URLs are set. If not, use defaults (customize as needed).
        if (raftNode.getPeerUrls() == null || raftNode.getPeerUrls().isEmpty()) {
            List<String> defaultPeers = Arrays.asList(
                "http://node1:8080", 
                "http://node2:8080", 
                "http://node3:8080", 
                "http://node4:8080", 
                "http://node5:8080"
            );
            raftNode.setPeerUrls(defaultPeers);
            System.out.println("Peer URLs set to defaults: " + defaultPeers);
        } else {
            System.out.println("Peer URLs already configured: " + raftNode.getPeerUrls());
        }

        // For a follower, no indices need initialization.
        // When the node later becomes leader, its transition logic should call initializeIndices().

        // Start the election timer so that this node will trigger an election if no heartbeats arrive.
        raftNode.resetElectionTimer();
        System.out.println("Election timer started.");
    }
}
