package com.example.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ReadIndexResponseDTO {
    private int readIndex;
}
