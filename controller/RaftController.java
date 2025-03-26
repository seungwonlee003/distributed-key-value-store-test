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

    @GetMapping("/get")
    public ResponseEntity<String> get(@RequestParam String key, 
                                      @RequestParam(defaultValue = "DEFAULT") String consistency) {
        try {
            ConsistencyLevel level = ConsistencyLevel.fromString(consistency);
            String value = consistencyService.read(key, level);
            return value != null ? ResponseEntity.ok(value) : ResponseEntity.notFound().build();
        } catch (ConsistencyException e) {
            return ResponseEntity.status(e.getStatus()).body(e.getMessage());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    @PostMapping("/insert")
    public ResponseEntity<String> insert(@RequestParam String key, @RequestParam String value) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry entry = new LogEntry(raftNode.getCurrentTerm(), key, value, LogEntry.Operation.INSERT);
        try {
            logManager.replicateLogToFollowers(Collections.singletonList(entry));
            return ResponseEntity.ok("Insert committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Insert failed: " + e.getMessage());
        }
    }

    @PostMapping("/update")
    public ResponseEntity<String> update(@RequestParam String key, @RequestParam String value) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry entry = new LogEntry(raftNode.getCurrentTerm(), key, value, LogEntry.Operation.UPDATE);
        try {
            logManager.replicateLogToFollowers(Collections.singletonList(entry));
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

        LogEntry entry = new LogEntry(raftNode.getCurrentTerm(), key, null, LogEntry.Operation.DELETE);
        try {
            logManager.replicateLogToFollowers(Collections.singletonList(entry));
            return ResponseEntity.ok("Delete committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Delete failed: " + e.getMessage());
        }
    }
}
