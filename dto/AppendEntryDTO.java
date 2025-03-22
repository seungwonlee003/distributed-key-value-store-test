package com.example.raft.dto;

import com.example.raft.log.LogEntry;

import java.util.List;

/**
 * Data Transfer Object for the AppendEntries RPC request in Raft.
 * Used by the leader to replicate log entries and send heartbeats to followers.
 */
public class AppendEntryDTO {
    private int term;           // Leader's term
    private int leaderId;       // So followers can redirect clients
    private int prevLogIndex;   // Index of log entry immediately preceding new ones
    private int prevLogTerm;    // Term of prevLogIndex entry
    private List<LogEntry> entries; // Log entries to store (empty for heartbeat)
    private int leaderCommit;   // Leader's commitIndex

    // Default constructor for serialization frameworks (e.g., Spring REST)
    public AppendEntryDTO() {
    }

    // Constructor with all fields
    public AppendEntryDTO(int term, int leaderId, int prevLogIndex, int prevLogTerm, 
                          List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    // Getters and Setters
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<LogEntry> entries) {
        this.entries = entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntryDTO{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", entries=" + (entries != null ? entries.size() : "null") +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
