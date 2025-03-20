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
    // Added dependency for heartbeat integration.
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

    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            startHeartbeats();
        }
    }

    // Leader now sends periodic AppendEntries (with an empty log) as heartbeats.
    private void startHeartbeats() {
        stopHeartbeats(); // Ensure any existing heartbeat task is stopped.
        if (state.getRole() != Role.LEADER) return;
        
        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                raftLogManager.sendHeartbeatToFollowers();
            }
        }, 0, 100, TimeUnit.MILLISECONDS); // Start immediately, repeat every 100ms.
    }
    
    private void stopHeartbeats() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }

    // Follower handling of AppendEntries (whether heartbeat or log replication)
    // is done in RaftLogManager.handleAppendEntries(), which resets the election timer.
    // For vote requests, see handleVoteRequest below.
    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        int currentTerm = state.getCurrentTerm();
        Integer votedFor = state.getVotedFor();

        if (requestTerm < currentTerm) {
            return new VoteResponseDTO(currentTerm, false);
        }
        if (requestTerm > currentTerm) {
            state.setCurrentTerm(requestTerm);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            resetElectionTimer();
            currentTerm = requestTerm;
        }
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

    private void resetElectionTimer() {
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
            if (state.getRole() == Role.LEADER) {
                return;
            }
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());

            int currentTerm = state.getCurrentTerm();
            List<CompletableFuture<VoteResponseDTO>> voteFutures = new ArrayList<>();

            for (String peerUrl : peerUrls) {
                voteFutures.add(requestVoteAsync(currentTerm, state.getNodeId(), 
                                                 state.getLastLogIndex(), state.getLastLogTerm(), 
                                                 peerUrl));
            }

            CompletableFuture.allOf(voteFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
                synchronized (this) {
                    if (state.getRole() != Role.CANDIDATE || state.getCurrentTerm() != currentTerm) {
                        return;
                    }
                    int voteCount = 1; // Self-vote.
                    for (CompletableFuture<VoteResponseDTO> future : voteFutures) {
                        try {
                            VoteResponseDTO response = future.get();
                            if (response.isVoteGranted()) {
                                voteCount++;
                            }
                        } catch (Exception e) {
                            // Ignore exception, count as no vote.
                        }
                    }
                    int totalNodes = peerUrls.size() + 1;
                    int majority = totalNodes / 2 + 1;
                    if (voteCount >= majority) {
                        becomeLeader();
                    } else {
                        resetElectionTimer();
                    }
                }
            }).exceptionally(ex -> {
                System.err.println("❌ Election failed: " + ex.getMessage());
                return null;
            });
        }
    }

    private CompletableFuture<VoteResponseDTO> requestVoteAsync(int term, int candidateId, 
                                                               int lastLogIndex, int lastLogTerm, 
                                                               String peerUrl) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = peerUrl + "/raft/vote";
                RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
                ResponseEntity<VoteResponseDTO> response = 
                        restTemplate.postForEntity(url, dto, VoteResponseDTO.class);
                VoteResponseDTO body = response.getBody();
                if (body == null) {
                    return new VoteResponseDTO(term, false);
                }
                synchronized (this) {
                    if (body.getTerm() > state.getCurrentTerm()) {
                        state.setCurrentTerm(body.getTerm());
                        state.setRole(Role.FOLLOWER);
                        state.setVotedFor(null);
                        resetElectionTimer();
                    }
                }
                return body;
            } catch (Exception e) {
                System.err.println("❌ Error requesting vote from " + peerUrl + ": " + e.getMessage());
                return new VoteResponseDTO(term, false);
            }
        }, asyncExecutor)
        .completeOnTimeout(new VoteResponseDTO(term, false), 100, TimeUnit.MILLISECONDS);
    }

    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        startHeartbeats();
    }
}
