@Service
public class RaftLogManager {
    private final RaftNode raftNode;
    private final RaftLog raftLog;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
    }

    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        // Validate log consistency with prevLogIndex and prevLogTerm
        // Append entries if consistent
        // Update commitIndex if needed
        // Return success or failure clearly
    }

    public void replicateLogEntryToFollowers(LogEntry entry) {
        // Leader sends AppendEntry RPC calls asynchronously to followers
        // Track follower responses
        // Commit the log entry if majority acknowledge
    }
}
