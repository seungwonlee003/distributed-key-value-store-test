package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for the RequestVote RPC request in Raft.
 * Sent by candidates to request votes from other nodes during leader election.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RequestVoteDTO {
    private int term;
    private int candidateId;
    private int lastLogIndex;
    private int lastLogTerm;
}
