package com.example.raft.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Centralized configuration for Raft algorithm, loaded from application.properties or YAML.
 */
@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "raft")
public class RaftConfig {

    // === Identity ===
    private Integer nodeId;

    // === Cluster Peers ===
    private List<String> peerUrls;

    // === Timers & Timeouts (based on Raft paper recommended ranges) ===
    private long electionTimeoutMillisMin;      // e.g., 150
    private long electionTimeoutMillisMax;      // e.g., 300
    private long heartbeatIntervalMillis;       // e.g., 50

    // === RPC and Client Behavior ===
    private long electionRpcTimeoutMillis;      // e.g., 300
    private long clientRequestTimeoutMillis;    // max wait before client gives up
    private long replicationBackoffMaxMillis;   // exponential backoff cap
}
