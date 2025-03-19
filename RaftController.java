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
    public ResponseEntity<VoteResponseDTO> requestVote(@RequestBody RequestVoteDTO request) {
        boolean voteGranted = raftNode.requestVote(request.getTerm(), request.getCandidateId(), 
                                                   request.getLastLogIndex(), request.getLastLogTerm(), null);
        return ResponseEntity.ok(new VoteResponseDTO(voteGranted, raftNode.getState().getCurrentTerm()));
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<Void> receiveHeartbeat(@RequestBody HeartbeatDTO heartbeat) {
        raftNode.receiveHeartbeat(heartbeat.getTerm());
        return ResponseEntity.ok().build();
    }
}
