package com.example.raft.dto;

/**
 * Data Transfer Object for the AppendEntries RPC response in Raft.
 * Sent by followers to the leader indicating success or failure of log replication.
 */
public class AppendEntryResponseDTO {
    private int term;    // Current term, for leader to update itself
    private boolean success; // True if follower contained entry matching prevLogIndex and prevLogTerm

    // Default constructor for serialization
    public AppendEntryResponseDTO() {
    }

    // Constructor with all fields
    public AppendEntryResponseDTO(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    // Getters and Setters
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "AppendEntryResponseDTO{" +
                "term=" + term +
                ", success=" + success +
                '}';
    }
}
