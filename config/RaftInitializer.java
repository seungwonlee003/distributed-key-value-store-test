@Configuration
@RequiredArgsConstructor
public class RaftInitializer {

    private final RaftNodeState nodeState;
    private final RaftStateManager stateManager;
    private final RaftNodeConfig config;

    @PostConstruct
    public void init() {
        nodeState.setNodeId(config.getNodeId());

        if (config.getPeerUrls() != null && !config.getPeerUrls().isEmpty()) {
            nodeState.setPeerUrls(config.getPeerUrls());
        } else {
            throw new IllegalStateException("Peer URLs must be defined in configuration.");
        }

        stateManager.becomeFollower(0);
    }
}

