import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private final int electionTimeoutMin = 150;
    private final int electionTimeoutMax = 300;
    private final RestTemplate restTemplate = new RestTemplate();

    // Store the current election timer to allow cancellation
    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> electionDeadline;

    // Async executor for handling parallel tasks
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

    /**
     * Periodically send heartbeats to maintain leadership.
     */
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
            return;
        }
        if (term == state.getCurrentTerm() && state.getRole() == Role.CANDIDATE) {
            state.setRole(Role.FOLLOWER);
        }
        resetElectionTimer();
    }
    
    private void resetElectionTimer() {
        cancelElectionTimerIfRunning();
        int timeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        electionFuture = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Handles an incoming vote request.
     */
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
            cancelElectionTimerIfRunning();
        }

        if (requestTerm == currentTerm && currentRole == Role.CANDIDATE) {
            state.setRole(Role.FOLLOWER);
            cancelElectionTimerIfRunning();
        }

        if (votedFor != null && !votedFor.equals(candidateId)) {
            return false;
        }

        int localLastTerm  = state.getLastLogTerm();
        int localLastIndex = state.getLastLogIndex();
        if (candidateLastTerm < localLastTerm || (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return false;
        }

        state.setVotedFor(candidateId);
        resetElectionTimer();
        return true;
    }

    /**
     * Reset the election timer to a new random timeout and schedule an election if it expires.
     */
    private void resetElectionTimer() {
        cancelElectionTimerIfRunning();
        int timeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        electionFuture = scheduler.schedule(this::startElection, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimerIfRunning() {
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(false);
        }
        if (electionDeadline != null && !electionDeadline.isDone()) {
            electionDeadline.cancel(false);
        }
    }

    /**
     * Start an election with timeout enforcement.
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
            AtomicInteger votes = new AtomicInteger(1);

            // Timeout for entire election process (to retry if we don’t get a majority)
            electionDeadline = scheduler.schedule(this::retryElection, electionTimeoutMax, TimeUnit.MILLISECONDS);

            for (String peerUrl : peerUrls) {
                requestVoteAsync(currentTerm, state.getNodeId(), state.getLastLogIndex(), state.getLastLogTerm(), peerUrl)
                        .thenAccept(granted -> {
                            if (granted) {
                                int totalVotes = votes.incrementAndGet();
                                synchronized (this) {
                                    if (totalVotes > peerUrls.size() / 2 &&
                                        state.getRole() == Role.CANDIDATE &&
                                        state.getCurrentTerm() == currentTerm) {
                                        becomeLeader();
                                    }
                                }
                            }
                        }).exceptionally(ex -> {
                            System.err.println("❌ Exception requesting vote: " + ex.getMessage());
                            return null;
                        });
            }
        }
    }
    
    /**
     * Asynchronously sends a RequestVote RPC to a peer with a timeout.
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
        .orTimeout(300, TimeUnit.MILLISECONDS) // Timeout per vote request
        .completeOnTimeout(false, 300, TimeUnit.MILLISECONDS); // Default false if timeout occurs
    }

    /**
     * Becomes a leader and starts sending heartbeats.
     */
    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }

    private void retryElection() {
        if (state.getRole() == Role.CANDIDATE) {
            int retryTimeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
            scheduler.schedule(this::startElection, retryTimeout, TimeUnit.MILLISECONDS);
        }
    }
}
