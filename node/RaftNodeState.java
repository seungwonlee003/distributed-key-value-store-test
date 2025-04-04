package com.example.raft;

import lombok.Getter;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Objects;

@Getter
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

    private final File stateFile = new File("raft_node_state.bin");

    public RaftNodeState(RaftConfig config) {
        this.nodeId = config.getNodeId();
        this.config = config;
        recoverFromDisk();
    }

    @PostConstruct
    private void init() {
        if (!stateFile.exists()) {
            persistToDisk();
        }
    }

    public void setCurrentTerm(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistToDisk();
        }
    }

    public void incrementTerm(){
        currentTerm++;
        persistToDisk();
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
            dos.writeInt(nodeId);
            dos.writeInt(currentTerm);
            dos.writeBoolean(votedFor != null);
            if (votedFor != null) {
                dos.writeInt(votedFor);
            }
            dos.writeInt(lastApplied);
            dos.flush();
            dos.getFD().sync(); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist RaftNodeState to disk", e);
        }
    }

    private void recoverFromDisk() {
        if (!stateFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(stateFile))) {
            int storedNodeId = dis.readInt();
            int term = dis.readInt();
            boolean hasVotedFor = dis.readBoolean();
            Integer voted = hasVotedFor ? dis.readInt() : null;
            int applied = dis.readInt();

            if (storedNodeId != nodeId) {
                throw new RuntimeException("Node ID mismatch: expected " + nodeId + ", got " + storedNodeId);
            }
            currentTerm = term;
            votedFor = voted;
            lastApplied = applied;
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover RaftNodeState from disk", e);
        }
    }
}
