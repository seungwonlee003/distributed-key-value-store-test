package com.example.raft.model;

public enum ConsistencyLevel {
    DEFAULT,
    READ_YOUR_WRITES,
    LEADER_LOCAL,
    EVENTUAL;
}
