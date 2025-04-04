package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ConfirmLeaderShipRequestDTO {
    private String nodeId;
    private int term;
}
