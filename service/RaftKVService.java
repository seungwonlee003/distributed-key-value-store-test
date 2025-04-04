@Service
@RequiredArgsConstructor
public class RaftKVService {
    private final RaftNode raftNode;
    private final RaftNodeState raftNodeState;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;
    private final RaftConfig raftConfig;
    private final RestTemplate restTemplate;

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
            } else {
                throw new IllegalStateException("Failed to obtain read index from leader. Status: " + response.getStatusCode());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Error while contacting leader for read index", e);
        }
    }

    public ReadIndexResponseDTO getSafeReadIndex() {
        if (!raftNodeState.isLeader()) {
            throw new IllegalStateException("Not leader. Cannot serve read index request.");
        }
        confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        return new ReadIndexResponseDTO(readIndex);
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
        confirmLeadership();
        int readIndex = raftLogManager.getCommitIndex();
        waitForLogToSync(readIndex);
        return kvStore.get(key);
    }

    private void confirmLeadership() {
        if (!raftNodeState.isLeader()) {
            throw new IllegalStateException("Not leader.");
        }

        int currentTerm = raftNodeState.getCurrentTerm();
        int confirmations = 1;

        for (String peerUrl : raftConfig.getPeerUrlList()) {
            try {
                HeartbeatResponseDTO response = restTemplate.postForObject(
                    peerUrl + "/raft/confirmLeadership",
                    new ConfirmLeadershipRequestDTO(raftNodeState.getNodeId(), currentTerm),
                    HeartbeatResponseDTO.class
                );
                if (response != null && response.isSuccess() && response.getTerm() == currentTerm) {
                    confirmations++;
                } else if (response != null && response.getTerm() > currentTerm) {
                    raftNodeState.becomeFollower(response.getTerm());
                    throw new IllegalStateException("Higher term detected, stepping down");
                }
            } catch (Exception e) {
            }
        }

        int totalNodes = raftConfig.getPeerUrlList().size() + 1;
        int majority = totalNodes / 2 + 1;
        if (confirmations < majority) {
            throw new IllegalStateException("Leadership not confirmed: quorum not achieved");
        }
    }

    public HeartbeatResponseDTO handleConfirmLeadership(ConfirmLeadershipRequestDTO request) {
        if (request.getTerm() > raftNodeState.getCurrentTerm()) {
            raftNodeState.becomeFollower(request.getTerm());
            return new HeartbeatResponseDTO(false, raftNodeState.getCurrentTerm());
        }
        if (raftNodeState.getCurrentRole() != Role.FOLLOWER) {
            return new HeartbeatResponseDTO(false, raftNodeState.getCurrentTerm());
        }
        boolean success = request.getTerm() == raftNodeState.getCurrentTerm();
        if (raftNodeState.getCurrentLeader() != null &&
            !raftNodeState.getCurrentLeader().equals(request.getNodeId())) {
            success = false;
        }
        return new HeartbeatResponseDTO(success, raftNodeState.getCurrentTerm());
    }
}
