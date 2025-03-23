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

    @PostMapping("/write")
    public ResponseEntity<String> write(@RequestBody String data) {
        if (raftNode.getRole() != Role.LEADER) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("Not leader");
        }

        LogEntry entry = new LogEntry(raftNode.getCurrentTerm(), data);

        try {
            raftLogManager.replicateLogToFollowers(Collections.singletonList(entry));
            return ResponseEntity.ok("Write committed");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("Write failed: " + e.getMessage());
        }
    }
}
