@Service
public class RaftLogManager {
    private final ClientRequestHandler clientRequestHandler;
    private final LogReplicator logReplicator;
    private final AppendEntriesHandler appendEntriesHandler;

    public RaftLogManager(
        RaftConfig config,
        RaftLog log,
        RaftStateManager stateManager,
        RaftNodeState nodeState,
        RestTemplate restTemplate,
        StateMachine stateMachine
    ) {
        this.clientRequestHandler = new ClientRequestHandler(log, nodeState, config);
        this.logReplicator = new LogReplicator(config, log, stateManager, nodeState, restTemplate, stateMachine);
        this.appendEntriesHandler = new AppendEntriesHandler(log, stateManager, nodeState, stateMachine);
    }

    public boolean handleClientRequest(LogEntry entry) {
        return clientRequestHandler.handle(entry);
    }

    public void startLogReplication() {
        logReplicator.start();
    }

    public AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        return appendEntriesHandler.handle(dto);
    }

    public void initializeIndices() {
        logReplicator.initializeIndices();
    }
}
