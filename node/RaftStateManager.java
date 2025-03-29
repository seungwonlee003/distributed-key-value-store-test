import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * RaftNode acts as the central coordinator for a server in the Raft consensus algorithm.
 *
 * It manages role transitions (leader, follower, candidate), tracks term and voting state.
 * 
 * RaftNode provides a clean interface to core Raft state so the client doesn't need to be aware of persistent and volatile states.
 */

@Service
@RequiredArgsConstructor
public class RaftStateManager {
    private final RaftNodeState state;
    private final HeartbeatManager heartbeatManager;
    private final ElectionTimer electionTimer;

    // =================== Raft Role Transitions =================== //

    public synchronized void becomeLeader() {
        state.setCurrentRole(Role.LEADER);
        System.out.printf("Node %s became leader for term %d%n", state.getNodeId(), state.getCurrentTerm());
        raftLogManager.initializeIndices();
        heartbeatManager.startHeartbeats();
    }

    public synchronized void becomeFollower(int newTerm) {
        state.setCurrentTerm(newTerm);
        state.setCurrentRole(Role.FOLLOWER);
        state.setVotedFor(null);
        heartbeatManager.stopHeartbeats();
        electionTimer.reset();
    }

    public void resetElectionTimer() {
        electionTimer.reset();
    }
}
