import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;
    private final RaftLogManager raftLogManager;
    private final ElectionManager electionManager;

    public RaftController(RaftNode raftNode, RaftLogManager raftLogManager, ElectionManager electionManager) {
        this.raftNode = raftNode;
        this.raftLogManager = raftLogManager;
        this.electionManager = electionManager;
    }

    @PostMapping("/requestVote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        VoteResponseDTO response = electionManager.handleVoteRequest(requestVoteDTO);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/appendEntries")
    public ResponseEntity<AppendEntryResponseDTO> appendEntries(@RequestBody AppendEntryDTO dto) {
        AppendEntryResponseDTO response = raftLogManager.handleAppendEntries(dto);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestParam String key, @RequestParam String value) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not the leader");
        }
    
        LogEntry clientEntry = new LogEntry(
            raftNode.getCurrentTerm(),
            key,
            value,
            LogEntry.Operation.INSERT
        );
    
        boolean committed = logManager.handleClientRequest(clientEntry);
    
        if (committed) {
            return ResponseEntity.ok("Insert committed");
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Insert failed (not committed or leadership lost)");
        }
    }
    
    @PostMapping("/insert")
    public CompletableFuture<ResponseEntity<String>> insert(@RequestParam String key, @RequestParam String value) {
        if (raftNode.getRole() != Role.LEADER) {
            return CompletableFuture.completedFuture(
                ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader")
            );
        }
    
        LogEntry clientEntry = new LogEntry(raftNode.getCurrentTerm(), key, value, LogEntry.Operation.INSERT);
        return logManager.handleClientRequest(clientEntry)
            .thenApply(committed -> {
                if (committed) {
                    return ResponseEntity.ok("Insert committed");
                } else {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Insert failed (not committed)");
                }
            }).exceptionally(e -> {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error: " + e.getMessage());
            });
    }

    @PostMapping("/update")
    public ResponseEntity<String> update(@RequestParam String key, @RequestParam String value) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry clientEntry = new LogEntry(raftNode.getCurrentTerm(), key, value, LogEntry.Operation.UPDATE);
        try {
            logManager.handleClientRequest(clientEntry);
            return ResponseEntity.ok("Update committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Update failed: " + e.getMessage());
        }
    }

    @PostMapping("/delete")
    public ResponseEntity<String> delete(@RequestParam String key) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry clientEntry = new LogEntry(raftNode.getCurrentTerm(), key, value, LogEntry.Operation.DELETE);
        try {
            logManager.handleClientRequest(clientEntry);
            return ResponseEntity.ok("Delete committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Delete failed: " + e.getMessage());
        }
    }
}
