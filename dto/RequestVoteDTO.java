package com.example.raft.dto;

/**
 * Data Transfer Object for the RequestVote RPC request in Raft.
 * Sent by candidates to request votes from other nodes during leader election.
 */
public class RequestVoteDTO {
    private int term;          // Candidate's term
    private int candidateId;   // Candidate requesting the vote
    private int lastLogIndex;  // Index of candidate’s last log entry
    private int lastLogTerm;   // Term of candidate’s last log entry

    // Default constructor for serialization
    public RequestVoteDTO() {
    }

    // Constructor with all fields
    public RequestVoteDTO(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    // Getters and Setters
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(int candidateId) {
        this.candidateId = candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public String toString() {
        return "RequestVoteDTO{" +
                "term=" + term +
                ", candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
