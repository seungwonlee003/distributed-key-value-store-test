package com.example.raft.dto;

/**
 * Data Transfer Object for the RequestVote RPC response in Raft.
 * Sent by nodes to the candidate indicating whether the vote was granted.
 */
public class VoteResponseDTO {
    private int term;         // Current term, for candidate to update itself
    private boolean voteGranted; // True if the candidate received the vote

    // Default constructor for serialization
    public VoteResponseDTO() {
    }

    // Constructor with all fields
    public VoteResponseDTO(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    // Getters and Setters
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public String toString() {
        return "VoteResponseDTO{" +
                "term=" + term +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
