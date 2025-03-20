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
}
