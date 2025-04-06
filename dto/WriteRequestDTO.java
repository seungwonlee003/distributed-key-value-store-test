package com.example.raft.dto;

import lombok.Data;

@Data
public class WriteRequestDTO {
    private String clientId;
    private long sequenceNumber;
    private String key;
    private String value; // For DELETE, this may be null or ignored.
}
