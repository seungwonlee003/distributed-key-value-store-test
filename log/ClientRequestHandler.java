@Component
@RequiredArgsConstructor
public class ClientRequestHandler {
    private final RaftLog raftLog;
    private final RaftNodeState raftNodeState;
    private final RaftConfig raftConfig;

    public boolean handle(LogEntry clientEntry) {
        raftLog.append(clientEntry);
        int entryIndex = raftLog.getLastIndex();
    
        long start = System.currentTimeMillis();
        long timeoutMillis = raftConfig.getClientRequestTimeoutMillis(); 

        // wait for at most 5 seconds for the client's write to be acknowledged by the majority
        while (raftNodeState.getRole() == Role.LEADER) {
            if (raftLog.getCommitIndex() >= entryIndex) {
                return true;
            }
            if (System.currentTimeMillis() - start > timeoutMillis) {
                return false;
            }
    
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    
        return false; 
    }
}
