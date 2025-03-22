import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;
    private final RaftLogManager raftLogManager;
    private final ElectionManager electionManager;

    public RaftController(RaftNode raftNode, RaftLogManager raftLogManager) {
        this.raftNode = raftNode;
        this.raftLogManager = raftLogManager;
    }

    @PostMapping("/requestVote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        VoteResponseDTO response = electionManager.handleVoteRequest(requestVoteDTO);
        return ResponseEntity.ok(response);
    }

    // Combined AppendEntries endpoint used both for heartbeats (empty entries) and log replication.
    @PostMapping("/appendEntries")
    public ResponseEntity<AppendEntryResponseDTO> appendEntries(@RequestBody AppendEntryDTO dto) {
        AppendEntryResponseDTO response = raftLogManager.handleAppendEntries(dto);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/write")
    public ResponseEntity<String> write(@RequestBody String data) {
        if (raftNode.getState().getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry entry = new LogEntry(raftLogManager.getRaftNodeState().getCurrentTerm(), data);

        try {
            // Now it's a blocking call. If it fails, we know replication didn't succeed
            raftLogManager.replicateLogToFollowers(Collections.singletonList(entry));
            return ResponseEntity.ok("Write committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Write failed: " + e.getMessage());
        }
    }
}
