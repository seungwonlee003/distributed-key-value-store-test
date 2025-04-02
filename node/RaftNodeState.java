package com.example.raft;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Objects;

@Getter
@Setter
@Component
public class RaftNodeState {
    private RaftConfig config;
    // Non-volatile state (persisted)
    private final int nodeId;
    private int currentTerm = 0;
    private Integer votedFor = null;
    private int lastApplied = 0;

    // Volatile state (not persisted)
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;

    private final File stateFile = new File("raft_node_state.bin");

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

    private synchronized void persistToDisk() {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(stateFile))) {
            dos.writeInt(nodeId);          // 4 bytes
            dos.writeInt(currentTerm);     // 4 bytes
            dos.writeBoolean(votedFor != null); // 1 byte
            if (votedFor != null) {
                dos.writeInt(votedFor);    // 4 bytes if present
            }
            dos.writeInt(lastApplied);     // 4 bytes
            dos.flush();                   // Push to OS buffer
            dos.getFD().sync();            // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist RaftNodeState to disk", e);
        }
    }

    private void recoverFromDisk() {
        if (stateFile.exists()) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(stateFile))) {
                int storedNodeId = dis.readInt();
                if (storedNodeId != nodeId) {
                    throw new RuntimeException("Node ID mismatch: expected " + nodeId + ", got " + storedNodeId);
                }
                currentTerm = dis.readInt();
                boolean hasVotedFor = dis.readBoolean();
                votedFor = hasVotedFor ? dis.readInt() : null;
                lastApplied = dis.readInt();
            } catch (IOException e) {
                throw new RuntimeException("Failed to recover RaftNodeState from disk", e);
            }
        }
    }
}
