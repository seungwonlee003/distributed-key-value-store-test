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

    private Integer nodeId;

    private Map<Integer, String> peerUrls;

    private long electionTimeoutMillisMin;      // 4000
    private long electionTimeoutMillisMax;      // 6000
    private long heartbeatIntervalMillis;       // 1000

    private long electionRpcTimeoutMillis;      // 2000
    private long clientRequestTimeoutMillis;    // 2000

    private boolean enableFollowerReads;
   
    public Collection<String> getPeerUrlList() {
        return peerUrls.values();
    }
}
