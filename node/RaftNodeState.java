package com.example.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@Component
public class RaftNodeState {
    private RaftConfig config;
    // Non-volatile state
    private final int nodeId;
    private int currentTerm = 0;
    private Integer votedFor = null;
    private int lastApplied = 0;

    // Volatile state
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;

    private final File stateFile = new File("raft_node_state.txt");
    private static final ObjectMapper mapper = new ObjectMapper();

    public RaftNodeState(int nodeId, RaftConfig config) {
        this.nodeId = nodeId;
        this.config = config;
        recoverFromDisk();
    }

    @PostConstruct
    private void init() {
        if (!stateFile.exists()) {
            persistToDisk(); // Write initial state if no file exists
        }
    }

    public void setCurrentTerm(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null; // Reset vote when term increases
            persistToDisk();
        }
    }

    public void setVotedFor(Integer votedFor) {
        this.votedFor = votedFor;
        persistToDisk();
    }

    public void setLastApplied(int lastApplied) {
        if (lastApplied > this.lastApplied) {
            this.lastApplied = lastApplied;
            persistToDisk();
        }
    }

    public void setCurrentLeader(Integer leaderId) {
        if (!Objects.equals(this.currentLeader, leaderId)) {
            System.out.println("New leader detected: Node " + leaderId);
        }
        this.currentLeader = leaderId;
    }

    public String getCurrentLeaderUrl() {
        return config.getPeerUrls().get(currentLeader);
    }

    public boolean isLeader() {
        return currentRole.equals(Role.LEADER);
    }

    private void persistToDisk() {
        try (FileOutputStream fos = new FileOutputStream(stateFile);
             FileChannel channel = fos.getChannel()) {
            // Create a map of the state to serialize as JSON
            Map<String, Object> state = new HashMap<>();
            state.put("nodeId", nodeId);
            state.put("currentTerm", currentTerm);
            state.put("votedFor", votedFor); // Null is preserved in JSON
            state.put("lastApplied", lastApplied);

            // Convert to JSON string
            String jsonState = mapper.writeValueAsString(state);
            byte[] bytes = jsonState.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            // Write to file and ensure durability
            channel.write(buffer);
            channel.force(true); // Replaces dos.getFD().sync()
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist RaftNodeState to disk", e);
        }
    }

    private void recoverFromDisk() {
        if (stateFile.exists()) {
            try (FileInputStream fis = new FileInputStream(stateFile)) {
                // Read the entire file into a byte array
                byte[] bytes = fis.readAllBytes();
                String jsonState = new String(bytes, StandardCharsets.UTF_8);

                // Parse JSON back into a map
                Map<String, Object> state = mapper.readValue(jsonState, Map.class);

                // Verify nodeId and set fields
                int storedNodeId = ((Number) state.get("nodeId")).intValue();
                if (storedNodeId != nodeId) {
                    throw new RuntimeException("Node ID mismatch: expected " + nodeId + ", got " + storedNodeId);
                }
                currentTerm = ((Number) state.get("currentTerm")).intValue();
                votedFor = state.get("votedFor") != null ? ((Number) state.get("votedFor")).intValue() : null;
                lastApplied = ((Number) state.get("lastApplied")).intValue();
            } catch (IOException | JsonProcessingException e) {
                throw new RuntimeException("Failed to recover RaftNodeState from disk", e);
            }
        }
    }
}
