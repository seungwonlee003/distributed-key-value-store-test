/**
 * Central coordinator for Raft log-related operations.
 * Acts as a Facade for client requests, log replication, and follower log handling.
 */

@Service
@RequiredArgsConstructor
public class RaftReplicationManager {
    private final ClientRequestHandler clientRequestHandler;
    private final LogReplicator logReplicator;
    private final AppendEntriesHandler appendEntriesHandler;

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
