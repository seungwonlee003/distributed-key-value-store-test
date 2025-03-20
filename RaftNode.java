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

    public RaftNode(RaftNodeState state, List<String> peerUrls) {
        this.state = state;
        this.peerUrls = peerUrls;
    }

    public RaftNodeState getState() {
        return state;
    }

    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            sendHeartbeats();
        }
    }

    private void startHeartbeats() {
            stopHeartbeats(); // Ensure any existing heartbeat task is stopped
            heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
                synchronized (this) {
                    if (state.getRole() == Role.LEADER) {
                        for (String peerUrl : peerUrls) {
                            sendHeartbeat(peerUrl); // Fixed typo
                        }
                    }
                }
            }, 0, 100, TimeUnit.MILLISECONDS); // Start immediately, repeat every 100ms
        }
    
        // Stop heartbeat scheduling (e.g., when stepping down)
        private void stopHeartbeats() {
            if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
                heartbeatFuture.cancel(false);
                heartbeatFuture = null;
            }
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

    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        int currentTerm = state.getCurrentTerm();
        Role currentRole = state.getRole();
        Integer votedFor = state.getVotedFor();

        // If the request term is stale, deny the vote
        if (requestTerm < currentTerm) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // If the request term is higher, update term and step down to follower
        if (requestTerm > currentTerm) {
            state.setCurrentTerm(requestTerm);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            resetElectionTimer();
            currentTerm = requestTerm; // Update local variable
        }

        // Deny vote if we've already voted for someone else
        if (votedFor != null && !votedFor.equals(candidateId)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // Check log up-to-date-ness
        int localLastTerm = state.getLastLogTerm();
        int localLastIndex = state.getLastLogIndex();
        if (candidateLastTerm < localLastTerm || 
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // Grant the vote
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
                    if (state.getRole() != Role.CANDIDATE || state.getCurrentTerm() != currentTerm) {
                        return; // Stepped down due to higher term or other reason
                    }

                    int voteCount = 1; // Include self-vote
                    for (CompletableFuture<VoteResponseDTO> future : voteFutures) {
                        try {
                            VoteResponseDTO response = future.get(); // Safe since all futures are done
                            if (response.isVoteGranted()) {
                                voteCount++;
                            }
                        } catch (Exception e) {
                            // Treat exceptions as no vote
                        }
                    }

                    // Become leader if majority votes received
                    if (voteCount > peerUrls.size() / 2) {
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
                    return new VoteResponseDTO(term, false); // Default to no vote
                }

                synchronized (this) {
                    if (body.getTerm() > state.getCurrentTerm()) {
                        state.setCurrentTerm(body.getTerm());
                        state.setRole(Role.FOLLOWER);
                        state.setVotedFor(null);
                        resetElectionTimer(); // Reset timer as follower, already contains cancel if running logic
                    }
                }
                return body;
            } catch (Exception e) {
                System.err.println("❌ Error requesting vote from " + peerUrl + ": " + e.getMessage());
                return new VoteResponseDTO(term, false); // Default to no vote on error
            }
        }, asyncExecutor)
        .completeOnTimeout(new VoteResponseDTO(term, false), 100, TimeUnit.MILLISECONDS);
    }

    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }
}
