import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;
    private final RaftLogManager raftLogManager;

    public RaftController(RaftNode raftNode, RaftLogManager raftLogManager) {
        this.raftNode = raftNode;
        this.raftLogManager = raftLogManager;
    }

    @PostMapping("/vote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        VoteResponseDTO response = raftNode.handleVoteRequest(requestVoteDTO);
        return ResponseEntity.ok(response);
    }

    // Combined AppendEntries endpoint used both for heartbeats (empty entries) and log replication.
    @PostMapping("/appendEntries")
    public ResponseEntity<AppendEntryResponseDTO> appendEntries(@RequestBody AppendEntryDTO dto) {
        AppendEntryResponseDTO response = raftLogManager.handleAppendEntries(dto);
        return ResponseEntity.ok(response);
    }
    
    @PostMapping("/write")
    public ResponseEntity<String> write(@RequestBody String command) {
        if (raftNode.getState().getRole() != Role.LEADER) {
            // Redirect to leader or reject if not leader
            Integer leaderId = raftNode.getState().getCurrentLeaderId(); // Assuming this exists or can be added
            if (leaderId != null) {
                String leaderUrl = raftNode.getPeerUrls().stream()
                    .filter(url -> url.contains(String.valueOf(leaderId)))
                    .findFirst()
                    .orElse(null);
                return ResponseEntity.status(307) // Temporary Redirect
                    .header("Location", leaderUrl + "/raft/write")
                    .body("Redirecting to leader: " + leaderUrl);
            }
            return ResponseEntity.status(503) // Service Unavailable
                .body("No leader available");
        }

        // Leader: Create log entry and replicate
        LogEntry entry = new LogEntry(raftNode.getState().getCurrentTerm(), command);
        raftLogManager.replicateLogToFollowers(Collections.singletonList(entry));

        // Return success (could wait for commit for stronger consistency)
        return ResponseEntity.ok("Write accepted: " + command);
    }
}
