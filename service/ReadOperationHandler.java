@Service
@RequiredArgsConstructor
public class ReadOperationHandler {
    private final RaftNodeState raftNodeState;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;
    private final RaftConfig raftConfig;
    private final RestTemplate restTemplate;
    private final LeadershipManager leadershipManager;

    public String handleRead(String key) throws IllegalStateException {
        leadershipManager.confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        waitForLogToSync(readIndex);
        return kvStore.get(key);
    }

    private void waitForLogToSync(int readIndex) {
        while (raftNodeState.getLastApplied() < readIndex) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while syncing log for read", e);
            }
        }
    }
}
