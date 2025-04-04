package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HeartbeatResponseDTO {
    private boolean success; // Indicates if the heartbeat was successful
    private int term; // The current term of the leader
}
