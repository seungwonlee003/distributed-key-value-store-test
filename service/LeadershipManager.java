@Service
@RequiredArgsConstructor
public class LeadershipManager {
    private final RaftNodeState raftNodeState;
    private final RaftLogManager raftLogManager;
    private final RaftConfig raftConfig;
    private final RestTemplate restTemplate;

    // wait for majority of votes
    public void confirmLeadership() {
        if (!raftNodeState.isLeader()) {
            throw new IllegalStateException("Not leader.");
        }
        int currentTerm = raftNodeState.getCurrentTerm();
        List<CompletableFuture<HeartbeatResponseDTO>> confirmationFutures = new ArrayList<>();
        ExecutorService executor = Executors.newCachedThreadPool();
    
        for (String peerUrl : raftConfig.getPeerUrlList()) {
            confirmationFutures.add(
                CompletableFuture.supplyAsync(() -> requestLeadershipConfirmation(
                    raftNodeState.getNodeId(), currentTerm, peerUrl
                ), executor)
                .orTimeout(raftConfig.getElectionRpcTimeoutMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> new HeartbeatResponseDTO(currentTerm, false))
            );
        }
    
        int totalNodes = raftConfig.getPeerUrlList().size() + 1;
        int majority = totalNodes / 2 + 1;
        int requiredPeerConfirmations = majority - 1; // Subtract self-confirmation
        CountDownLatch latch = new CountDownLatch(requiredPeerConfirmations);
    
        for (CompletableFuture<HeartbeatResponseDTO> future : confirmationFutures) {
            future.thenAccept(response -> {
                synchronized (this) {
                    if (!raftNodeState.isLeader() || raftNodeState.getCurrentTerm() != currentTerm) {
                        return;
                    }
                    if (response != null && response.isSuccess() && response.getTerm() == currentTerm) {
                        latch.countDown();
                    }
                }
            });
        }

        try {
            latch.await(raftConfig.getElectionRpcTimeoutMillis() * 2, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while confirming leadership.");
        }
    
        if (latch.getCount() > 0) {
            throw new IllegalStateException("Leadership confirmation failed.");
        }
    }
    private HeartbeatResponseDTO requestLeadershipConfirmation(int leaderId, int term, String peerUrl) {
        try {
            String url = peerUrl + "/raft/confirmLeadership";
            ConfirmLeadershipRequestDTO dto = new ConfirmLeadershipRequestDTO(leaderId, term);
            ResponseEntity<HeartbeatResponseDTO> response = restTemplate.postForEntity(url, dto, HeartbeatResponseDTO.class);
            HeartbeatResponseDTO body = response.getBody() != null ? response.getBody() : new HeartbeatResponseDTO(term, false);
            if (body.getTerm() > raftNodeState.getCurrentTerm()) {
                raftNodeState.becomeFollower(body.getTerm());
            }
            return body;
        } catch (Exception e) {
            return new HeartbeatResponseDTO(term, false);
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
        boolean success = request.getTerm() == raftNodeState.getCurrentTerm() &&
                         (raftNodeState.getCurrentLeader() == null ||
                          raftNodeState.getCurrentLeader().equals(request.getNodeId()));
        return new HeartbeatResponseDTO(success, raftNodeState.getCurrentTerm());
    }
}
