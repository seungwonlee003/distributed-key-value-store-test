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
    private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    public RaftNode(RaftNodeState state, List<String> peerUrls) {
        this.state = state;
        this.peerUrls = peerUrls;
    }

    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            sendHeartbeats();
        }
    }

    private void sendHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                if (state.getRole() == Role.LEADER) {
                    for (String peerUrl : peerUrls) {
                        sendHeartbeat(peerUrl);
                    }
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat(String peerUrl) {
        try {
            String url = peerUrl + "/raft/heartbeat";
            HeartbeatDTO hb = new HeartbeatDTO(state.getCurrentTerm(), state.getNodeId());
            restTemplate.postForEntity(url, hb, Void.class);
        } catch (Exception e) {
            System.err.println("❌ Error sending heartbeat to " + peerUrl + ": " + e.getMessage());
        }
    }

    public synchronized void receiveHeartbeat(int term) {
        if (term > state.getCurrentTerm()) {
            state.setCurrentTerm(term);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            resetElectionTimer();
        } else if (term == state.getCurrentTerm()) {
            if (state.getRole() == Role.CANDIDATE) {
                state.setRole(Role.FOLLOWER);
            }
            resetElectionTimer();
        }
    }

    public synchronized boolean handleVoteRequest(RequestVoteDTO requestVote) {
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        int currentTerm = state.getCurrentTerm();
        Role currentRole = state.getRole();
        Integer votedFor = state.getVotedFor();

        if (requestTerm < currentTerm) {
            return false;
        }

        if (requestTerm > currentTerm) {
            state.setCurrentTerm(requestTerm);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            resetElectionTimer();
        }

        if (requestTerm == currentTerm && currentRole == Role.CANDIDATE) {
            state.setRole(Role.FOLLOWER);
            resetElectionTimer();
        }

        if (votedFor != null && !votedFor.equals(candidateId)) {
            return false;
        }

        int localLastTerm = state.getLastLogTerm();
        int localLastIndex = state.getLastLogIndex();
        if (candidateLastTerm < localLastTerm || 
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return false;
        }

        state.setVotedFor(candidateId);
        resetElectionTimer();
        return true;
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

    /**
     * Initiates an election by sending vote requests to all peers and waiting for all responses
     * (or timeouts) before deciding leadership. This prevents brief leadership if a higher-term
     * response arrives late.
     */
    private void startElection() {
        synchronized (this) {
            if (state.getRole() != Role.FOLLOWER && state.getRole() != Role.CANDIDATE) {
                return;
            }
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());

            int currentTerm = state.getCurrentTerm();
            List<CompletableFuture<Boolean>> voteFutures = new ArrayList<>();

            // Collect vote futures from all peers
            for (String peerUrl : peerUrls) {
                voteFutures.add(requestVoteAsync(currentTerm, state.getNodeId(), 
                                                 state.getLastLogIndex(), state.getLastLogTerm(), 
                                                 peerUrl));
            }

            // Wait for all vote requests to complete or timeout
            CompletableFuture.allOf(voteFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
                synchronized (this) {
                    // Ensure still a candidate in the same term after all responses
                    if (state.getRole() == Role.CANDIDATE && state.getCurrentTerm() == currentTerm) {
                        int voteCount = 1; // Include self-vote
                        for (CompletableFuture<Boolean> future : voteFutures) {
                            try {
                                if (future.get()) { // Safe to call get() since all futures are done
                                    voteCount++;
                                }
                            } catch (Exception e) {
                                // Treat exceptions as false votes
                            }
                        }
                        // Become leader if majority votes received
                        if (voteCount > peerUrls.size() / 2) {
                            becomeLeader();
                        } else {
                            // Did not win, reset timer for next election attempt
                            resetElectionTimer();
                        }
                    }
                    // If role or term changed (e.g., stepped down), do nothing
                }
            }).exceptionally(ex -> {
                System.err.println("❌ Election failed: " + ex.getMessage());
                return null;
            });
        }
    }

    /**
     * Sends a vote request to a peer asynchronously with a 300ms timeout.
     * Steps down to follower if a higher term is received.
     */
    private CompletableFuture<Boolean> requestVoteAsync(int term, int candidateId, 
                                                        int lastLogIndex, int lastLogTerm, 
                                                        String peerUrl) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = peerUrl + "/raft/vote";
                RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
                ResponseEntity<VoteResponseDTO> response = 
                        restTemplate.postForEntity(url, dto, VoteResponseDTO.class);

                VoteResponseDTO body = response.getBody();
                if (body == null) return false;

                synchronized (this) {
                    if (body.getTerm() > state.getCurrentTerm()) {
                        state.setCurrentTerm(body.getTerm());
                        state.setRole(Role.FOLLOWER);
                        state.setVotedFor(null);
                        cancelElectionTimerIfRunning();
                        return false;
                    }
                }
                return body.isVoteGranted();
            } catch (Exception e) {
                System.err.println("❌ Error requesting vote from " + peerUrl + ": " + e.getMessage());
                return false;
            }
        }, asyncExecutor)
        .completeOnTimeout(false, 300, TimeUnit.MILLISECONDS); // Timeout after 300ms
    }

    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }
}
