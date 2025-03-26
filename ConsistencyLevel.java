package com.example.raft.model;

public enum ConsistencyLevel {
    DEFAULT,
    LEADER_LOCAL,
    READ_YOUR_WRITES,
    EVENTUAL;
}
