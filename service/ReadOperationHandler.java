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
        if (raftConfig.isEnableFollowerReads()) {
            if (!raftNodeState.isLeader()) {
                int readIndex = requestReadIndexFromLeader();
                waitForLogToSync(readIndex);
                return kvStore.get(key);
            } else {
                return performLeaderRead(key);
            }
        } else {
            if (!raftNodeState.isLeader()) {
                String leaderUrl = raftNodeState.getCurrentLeaderUrl();
                throw new IllegalStateException("Read requests must be routed to the leader at " + leaderUrl + " when follower reads are disabled");
            }
            return performLeaderRead(key);
        }
    }

    private int requestReadIndexFromLeader() {
        String leaderUrl = raftNodeState.getCurrentLeaderUrl();
        if (leaderUrl == null) {
            throw new IllegalStateException("Leader unknown. Cannot perform follower read.");
        }
        try {
            ResponseEntity<ReadIndexResponseDTO> response = restTemplate.getForEntity(
                leaderUrl + "/raft/rpc/readIndex",
                ReadIndexResponseDTO.class
            );
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return response.getBody().getReadIndex();
            }
            throw new IllegalStateException("Failed to obtain read index from leader. Status: " + response.getStatusCode());
        } catch (Exception e) {
            throw new IllegalStateException("Error while contacting leader for read index", e);
        }
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

    private String performLeaderRead(String key) {
        leadershipManager.confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        waitForLogToSync(readIndex);
        return kvStore.get(key);
    }
}
