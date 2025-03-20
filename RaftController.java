import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;

    public RaftController(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @PostMapping("/vote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        VoteResponseDTO response = raftNode.handleVoteRequest(requestVoteDTO);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> heartbeat(@RequestBody HeartbeatDTO heartbeatDTO) {
        raftNode.receiveHeartbeat(heartbeatDTO.getTerm());
        return ResponseEntity.ok().build();
    }

    // assumes only leader receives appendEntry request
    @PostMapping("/appendEntry")
    public AppendEntryResponseDTO appendEntry(@RequestBody AppendEntryDTO dto) {
        return logManager.handleAppendEntries(dto);
    }
}
