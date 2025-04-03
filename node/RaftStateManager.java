import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * RaftStateManager manages role transitions (leader, follower, candidate).
 */

@Service
@RequiredArgsConstructor
public class RaftStateManager {
    private final RaftNodeState state;
    private final HeartbeatManager heartbeatManager;
    private final ElectionTimer electionTimer;
    private final RaftReplicationManager raftReplicationManager;

    public synchronized void becomeLeader() {
        state.setCurrentRole(Role.LEADER);
        System.out.printf("Node %s became leader for term %d%n", state.getNodeId(), state.getCurrentTerm());
        raftReplicationManager.initializeIndices();
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
