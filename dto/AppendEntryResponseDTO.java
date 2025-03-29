package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for the AppendEntries RPC response in Raft.
 * Sent by followers to indicate success or failure of log replication.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntryResponseDTO {
    private int term;
    private boolean success;
}
