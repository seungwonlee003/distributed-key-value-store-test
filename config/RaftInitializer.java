@Configuration
@RequiredArgsConstructor
public class RaftInitializer {

    private final RaftNodeState nodeState;
    private final RaftNodeConfig config;

    @PostConstruct
    public void init() {
        nodeState.setNodeId(config.getNodeId());
        nodeState.setRole(Role.FOLLOWER);
        nodeState.setCurrentTerm(0);
        nodeState.setVotedFor(null);

        System.out.println("Node " + nodeState.getNodeId() + " starting as FOLLOWER");

        if (config.getPeerUrls() != null && !config.getPeerUrls().isEmpty()) {
            nodeState.setPeerUrls(config.getPeerUrls());
            System.out.println("Peer URLs set from config: " + config.getPeerUrls());
        } else {
            throw new IllegalStateException("Peer URLs must be defined in configuration.");
        }

        nodeState.resetElectionTimer();
        System.out.println("Election timer started.");
    }
}

