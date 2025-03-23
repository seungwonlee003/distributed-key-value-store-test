import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Service
public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final RestTemplate restTemplate;
    private final ExecutorService asyncExecutor;
    private final RaftLog raftLog;
    private final RaftLogManager raftLogManager;
    private final StateMachine stateMachine;
    private final ElectionManager electionManager;
    private final HeartbeatManager heartbeatManager;
    private final ElectionTimer electionTimer;

    @Autowired
    public RaftNode(RaftNodeState state,
                    List<String> peerUrls,
                    RestTemplate restTemplate,
                    RaftLog raftLog,
                    ExecutorService asyncExecutor,
                    RaftLogManager raftLogManager,
                    StateMachine stateMachine,
                    ElectionManager electionManager,
                    HeartbeatManager heartbeatManager,
                    ElectionTimer electionTimer) {
        this.state = state;
        this.peerUrls = peerUrls;
        this.restTemplate = restTemplate;
        this.asyncExecutor = asyncExecutor;
        this.raftLog = raftLog;
        this.raftLogManager = raftLogManager;
        this.stateMachine = stateMachine;
        this.electionManager = electionManager;
        this.heartbeatManager = heartbeatManager;
        this.electionTimer = electionTimer;
    }

    private synchronized void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        raftLogManager.initializeIndices();
        heartbeatManager.startHeartbeats();
    }

    public synchronized void becomeFollower(int newTerm) {
        state.setCurrentTerm(newTerm);
        state.setRole(Role.FOLLOWER);
        state.setVotedFor(null);
        heartbeatManager.stopHeartbeats();
        electionTimer.reset();
    }

// Control Method
    public void resetElectionTimer() {
        electionTimer.reset();
    }

    // Facade Getters
    public int getCurrentTerm() {
        return state.getCurrentTerm();
    }

    public Role getRole() {
        return state.getRole();
    }

    public String getNodeId() {
        return state.getNodeId();
    }

    public int getLastApplied() {
        return state.getLastApplied();
    }

    public void setLastApplied(int index) {
        state.setLastApplied(index);
    }

    public List<String> getPeerUrls() {
        return peerUrls;
    }

    public ExecutorService getAsyncExecutor() {
        return asyncExecutor;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    // Temporary Direct State Access (required by ElectionManager)
    public RaftNodeState getState() {
        return state;
    }
}
