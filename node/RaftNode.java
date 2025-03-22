import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(4);
    private final RaftLogManager raftLogManager;
    private final StateMachine stateMachine;
    private final ElectionManager electionManager;
    private final HeartbeatManager heartbeatManager;

    public RaftNode(RaftNodeState state, List<String> peerUrls, RaftLogManager raftLogManager, StateMachine stateMachine) {
        this.state = state;
        this.peerUrls = peerUrls;
        this.raftLogManager = raftLogManager;
        this.stateMachine = stateMachine != null ? stateMachine : new InMemoryStateMachine(); // Default
        this.electionManager = new ElectionManager(this);
        this.heartbeatManager = new HeartbeatManager(this);
    }

    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            electionManager.resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            becomeLeader();
        }
    }

    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        return electionManager.handleVoteRequest(requestVote);
    }

    public void resetElectionTimer() {
        electionManager.resetElectionTimer();
    }

    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        raftLogManager.initializeIndices();
        heartbeatManager.startHeartbeats();
    }

    // Getters
    public RaftNodeState getState() { return state; }
    public List<String> getPeerUrls() { return peerUrls; }
    public RestTemplate getRestTemplate() { return restTemplate; }
    public ExecutorService getAsyncExecutor() { return asyncExecutor; }
    public RaftLogManager getRaftLogManager() { return raftLogManager; }
    public StateMachine getStateMachine() { return stateMachine; }
}
