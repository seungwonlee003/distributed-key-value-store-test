import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final RestTemplate restTemplate;
    private final ExecutorService asyncExecutor;
    private final RaftLogManager raftLogManager;
    private final StateMachine stateMachine;
    private final ElectionManager electionManager;
    private final HeartbeatManager heartbeatManager;

    @Autowired
    public RaftNode(RaftNodeState state, List<String> peerUrls, RestTemplate restTemplate, ExecutorService asyncExecutor, RaftLogManager raftLogManager, 
                    StateMachine stateMachine, ElectionManager electionManager, HeartbeatManager heartbeatManager) {
        this.state = state;
        this.peerUrls = peerUrls;
        this.restTemplate = restTemplate;
        this.asyncExecutor = asyncExecutor;
        this.raftLogManager = raftLogManager;
        this.stateMachine = stateMachine;
        this.electionManager = electionManager;
        this.heartbeatManager = heartbeatManager;
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
