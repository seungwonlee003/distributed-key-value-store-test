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

        while (raftNodeState.getRole() == Role.LEADER) {
            if (raftNodeState.getLastApplied() >= entryIndex) {
                return true;
            }
            if (System.currentTimeMillis() - start > timeoutMillis) {
                return false;
            }
    
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    
        return false; 
    }
}
