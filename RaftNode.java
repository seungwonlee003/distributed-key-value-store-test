import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private final int electionTimeoutMin = 150;
    private final int electionTimeoutMax = 300;
    private final RestTemplate restTemplate = new RestTemplate();
    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> heartbeatFuture;
    private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(4);
    private final RaftLogManager raftLogManager;

    public RaftNode(RaftNodeState state, List<String> peerUrls, RaftLogManager raftLogManager) {
        this.state = state;
        this.peerUrls = peerUrls;
        this.raftLogManager = raftLogManager;
    }

    public RaftNodeState getState() {
        return state;
    }

    public List<String> getPeerUrls() {
        return peerUrls;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public ExecutorService getAsyncExecutor() {
        return asyncExecutor;
    }

    // Initialize node state on startup (Raft Section 5.2).
    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            becomeLeader(); // Ensure heartbeats start for pre-elected leader.
        }
    }

    private void startHeartbeats() {
        stopHeartbeats();
        if (state.getRole() != Role.LEADER) return;
    
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                if (state.getRole() == Role.LEADER) {
                    raftLogManager.replicateLogToFollowers(null); // Heartbeat with no new entries.
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeats() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }

    // Handles vote requests from candidates (Raft Section 5.2).
    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        int currentTerm = state.getCurrentTerm();
        Integer votedFor = state.getVotedFor();

        // Reject if candidate’s term is outdated.
        if (requestTerm < currentTerm) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // Step down if a higher term is observed.
        if (requestTerm > currentTerm) {
            state.setCurrentTerm(requestTerm);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            stopHeartbeats();
            resetElectionTimer();
            currentTerm = requestTerm;
        }

        // Vote only if we haven’t voted for someone else and candidate’s log is up-to-date.
        if (votedFor != null && !votedFor.equals(candidateId)) {
            return new VoteResponseDTO(currentTerm, false);
        }
        int localLastTerm = state.getLastLogTerm();
        int localLastIndex = state.getLastLogIndex();
        if (candidateLastTerm < localLastTerm || 
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        state.setVotedFor(candidateId);
        resetElectionTimer();
        return new VoteResponseDTO(currentTerm, true);
    }

    // Resets election timer with a random timeout (Raft Section 5.2).
    public void resetElectionTimer() {
        cancelElectionTimerIfRunning();
        int timeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        electionFuture = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimerIfRunning() {
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(false);
        }
    }
    
    private void startElection() {
        synchronized (this) {
            if (state.getRole() == Role.LEADER) return;
    
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());
    
            int currentTerm = state.getCurrentTerm();
            List<CompletableFuture<VoteResponseDTO>> voteFutures = new ArrayList<>();
            ExecutorService executor = raftNode.getAsyncExecutor(); // Added executor from raftNode
            long voteTimeoutMs = 500; // Example timeout (adjust as needed)
    
            // Submit vote requests asynchronously with per-task timeout
            for (String peerUrl : peerUrls) {
                CompletableFuture<VoteResponseDTO> voteFuture = CompletableFuture
                    .supplyAsync(() -> requestVote(currentTerm, state.getNodeId(), 
                                                        state.getLastLogIndex(), state.getLastLogTerm(), peerUrl))
                    .orTimeout(voteTimeoutMs, TimeUnit.MILLISECONDS) // Per-task timeout
                    .exceptionally(throwable -> {
                        // Timeout or failure results in no vote
                        return new VoteResponseDTO(false, currentTerm); // Return a "no vote" response
                    });
                voteFutures.add(voteFuture);
            }
    
            // Wait for all vote requests to complete, then process results
            CompletableFuture.allOf(voteFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
                synchronized (this) {
                    if (state.getRole() != Role.CANDIDATE || state.getCurrentTerm() != currentTerm) {
                        return; // Term or role changed; abort.
                    }
                    int voteCount = 1; // Self-vote
                    for (CompletableFuture<VoteResponseDTO> future : voteFutures) {
                        try {
                            VoteResponseDTO response = future.get(); // Safe to call get() since allOf ensures completion
                            if (response != null && response.isVoteGranted()) {
                                voteCount++;
                            }
                        } catch (Exception e) {
                            // Handle any unexpected errors (e.g., interrupted execution)
                        }
                    }
                    int majority = (peerUrls.size() + 1) / 2 + 1;
                    if (voteCount >= majority) {
                        becomeLeader();
                    } else {
                        resetElectionTimer(); // Retry election
                    }
                }
            }).exceptionally(ex -> {
                System.err.println("Election failed unexpectedly: " + ex.getMessage());
                return null;
            });
        }
    }

    public VoteResponseDTO requestVote(
        int term, int candidateId, int lastLogIndex, int lastLogTerm, String peerUrl
    ) {
        try {
            // Build the URL for the peer's /vote endpoint
            String url = peerUrl + "/raft/vote";
    
            RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
    
            ResponseEntity<VoteResponseDTO> response =
                    restTemplate.postForEntity(url, dto, VoteResponseDTO.class);
    
            VoteResponseDTO body = (response.getBody() != null)
                    ? response.getBody()
                    : new VoteResponseDTO(term, false);
    
            // Check if the returned term is higher than our current term
            synchronized (this) {
                if (body.getTerm() > state.getCurrentTerm()) {
                    state.setCurrentTerm(body.getTerm());
                    state.setRole(Role.FOLLOWER);
                    state.setVotedFor(null);
                    stopHeartbeats();  // If you want to ensure no leftover heartbeats
                    resetElectionTimer();
                }
            }
            return body;
    
        } catch (Exception e) {
            // On error, we consider the vote not granted
            return new VoteResponseDTO(term, false);
        }
    }
    public VoteResponseDTO requestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm, String peerUrl) {
        try {
            String url = peerUrl + "/raft/vote";
            RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
            ResponseEntity<VoteResponseDTO> response =
                    restTemplate.postForEntity(url, dto, VoteResponseDTO.class);
            return (response.getBody() != null) ? response.getBody() : new VoteResponseDTO(term, false);
        } catch (Exception e) {
            return new VoteResponseDTO(term, false);
        }
    }

    // Transitions node to leader state (Raft Section 5.2).
    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        raftLogManager.initializeIndices(); // Initialize replication indices.
        startHeartbeats(); // Begin broadcasting heartbeats.
    }
}
