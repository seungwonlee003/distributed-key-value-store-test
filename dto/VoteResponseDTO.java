package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for the RequestVote RPC response in Raft.
 * Sent by nodes to the candidate indicating whether the vote was granted.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VoteResponseDTO {
    private int term;
    private boolean voteGranted;
}
