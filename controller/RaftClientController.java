package com.example.raft.client;

import com.example.raft.log.LogEntry;
import com.example.raft.log.LogEntry.Operation;
import com.example.raft.dto.WriteRequestDTO;
import com.example.raft.node.RaftNode;
import com.example.raft.log.RaftLogManager;
import com.example.raft.operation.ReadOperationHandler;
import com.example.raft.state.Role;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/raft/client")
public class RaftClientController {

    private final RaftNode raftNode;
    private final RaftLogManager logManager;
    private final ReadOperationHandler readOperationHandler;
    
    @GetMapping("/get")
    public ResponseEntity<String> read(@RequestParam String key) {
        String val = readOperationHandler.handleRead(key);
        return ResponseEntity.ok(val);
    }

    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestBody WriteRequestDTO request) {
        return handleWrite(request, Operation.INSERT, "Insert");
    }

    @PostMapping("/update")
    public ResponseEntity<String> update(@RequestBody WriteRequestDTO request) {
        return handleWrite(request, Operation.UPDATE, "Update");
    }

    @PostMapping("/delete")
    public ResponseEntity<String> delete(@RequestBody WriteRequestDTO request) {
        return handleWrite(request, Operation.DELETE, "Delete");
    }

    private ResponseEntity<String> handleWrite(WriteRequestDTO request, Operation op, String label) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not the leader");
        }
        
        LogEntry entry = new LogEntry(
            raftNode.getCurrentTerm(),
            request.getKey(),
            request.getValue(),
            op,
            request.getClientId(),
            request.getSequenceNumber()
        );
        
        boolean committed = logManager.handleClientRequest(entry);
        if (committed) {
            return ResponseEntity.ok(label + " committed");
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(label + " failed (not committed or leadership lost)");
        }
    }
}
