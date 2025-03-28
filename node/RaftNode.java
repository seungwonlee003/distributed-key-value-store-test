import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * RaftNode acts as the central coordinator for a server in the Raft consensus algorithm.
 *
 * It manages role transitions (leader, follower, candidate), tracks term and voting state.
 * 
 * RaftNode provides a clean interface to core Raft state while enforcing encapsulation and separation of concerns.
 */

@Service
public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final RestTemplate restTemplate;
    private final ExecutorService asyncExecutor;
    private final RaftLog raftLog;
    private final RaftLogManager raftLogManager;
    private final StateMachine stateMachine;
    private final HeartbeatManager heartbeatManager;
    private final ElectionTimer electionTimer;

    public RaftNode(RaftNodeState state,
                    List<String> peerUrls,
                    RestTemplate restTemplate,
                    ExecutorService asyncExecutor,
                    RaftLog raftLog,
                    RaftLogManager raftLogManager,
                    StateMachine stateMachine,
                    HeartbeatManager heartbeatManager,
                    ElectionTimer electionTimer) {
        this.state = state;
        this.peerUrls = peerUrls;
        this.restTemplate = restTemplate;
        this.asyncExecutor = asyncExecutor;
        this.raftLog = raftLog;
        this.raftLogManager = raftLogManager;
        this.stateMachine = stateMachine;
        this.heartbeatManager = heartbeatManager;
        this.electionTimer = electionTimer;
    }

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

    // =================== Raft State Access =================== //

    public int getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public void incrementTerm() {
        state.setCurrentTerm(state.getCurrentTerm() + 1);
    }

    public Role getCurrentRole() {
        return state.getCurrentRole();
    }

    public void setCurrentRole(Role role) {
        state.setCurrentRole(role);
    }

    public Integer getNodeId() {
        return state.getNodeId();
    }

    public Integer getVotedFor() {
        return state.getVotedFor();
    }

    public void setVotedFor(Integer votedFor) {
        state.setVotedFor(votedFor);
    }

    public int getLastApplied() {
        return state.getLastApplied();
    }

    public void setLastApplied(int index) {
        state.setLastApplied(index);
    }

    public Integer getCurrentLeader() {
        return state.getCurrentLeader();
    }

    public void setCurrentLeader(Integer leaderId) {
        state.setCurrentLeader(leaderId);
    }

    // =================== Topology & Infrastructure Access =================== //

    public List<String> getPeerUrls() {
        return peerUrls;

    public ExecutorService getAsyncExecutor() {
        return asyncExecutor;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }
}
