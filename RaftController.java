import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftNode raftNode;

    // Inject the RaftNode (configured as a Spring bean) via constructor.
    public RaftController(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    /**
     * Endpoint for receiving a vote request.
     * When a candidate node sends a vote request, this endpoint handles it.
     */
    @PostMapping("/vote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        // Process the vote request using a method defined in RaftNode.
        boolean voteGranted = raftNode.handleVoteRequest(requestVoteDTO);
        int currentTerm = raftNode.getState().getCurrentTerm();
        return ResponseEntity.ok(new VoteResponseDTO(voteGranted, currentTerm));
    }

    /**
     * Endpoint for receiving heartbeats.
     * A leader sends heartbeats to followers to keep them in follower mode.
     */
    @PostMapping("/heartbeat")
    public ResponseEntity<Void> heartbeat(@RequestBody HeartbeatDTO heartbeatDTO) {
        raftNode.receiveHeartbeat(heartbeatDTO.getTerm());
        return ResponseEntity.ok().build();
    }
}
