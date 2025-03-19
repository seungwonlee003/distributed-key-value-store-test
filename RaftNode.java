import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;

public class RaftNode {
    private final RaftNodeState state;
    private final List<String> peerUrls; // List of peer node URLs (e.g., "http://node2:8080")
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final Random random = new Random();
    private final int electionTimeoutMin = 150;
    private final int electionTimeoutMax = 300;
    private final RestTemplate restTemplate = new RestTemplate();
    
    // Store the current election timer task to allow cancellation
    private ScheduledFuture<?> electionFuture;

    public RaftNode(RaftNodeState state, List<String> peerUrls) {
        this.state = state;
        this.peerUrls = peerUrls;
    }

    public void start() {
        if (state.getRole() == Role.FOLLOWER) {
            resetElectionTimer();
        }
    }

    /**
     * Resets the election timer. If a previous timer is pending, it gets canceled.
     * The new timer is set with a random timeout.
     */
    private void resetElectionTimer() {
        // Cancel the previous election timer if it's still pending
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(false);
        }
        int timeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        electionFuture = scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    /** Triggers Election if No Heartbeat is Received */
    private void startElection() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());
            int votes = 1; // Vote for itself

            System.out.println("🗳️ Node " + state.getNodeId() + " starting election for term " + state.getCurrentTerm());

            for (String peerUrl : peerUrls) {
                if (requestVote(state.getCurrentTerm(), state.getNodeId(),
                        state.getLastLogIndex(), state.getLastLogTerm(), peerUrl)) {
                    votes++;
                }
            }

            if (votes > peerUrls.size() / 2) {
                becomeLeader();
            } else {
                retryElection();
            }
        }
    }

    /** Sends an HTTP request to request a vote from a peer */
    public boolean requestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm, String peerUrl) {
        try {
            String url = peerUrl + "/raft/vote"; // e.g., "http://node2:8080/raft/vote"
            RequestVoteDTO request = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
            ResponseEntity<VoteResponseDTO> response = restTemplate.postForEntity(url, request, VoteResponseDTO.class);
            return response.getBody() != null && response.getBody().isVoteGranted();
        } catch (Exception e) {
            System.err.println("❌ Error requesting vote from " + peerUrl + ": " + e.getMessage());
            return false;
        }
    }

    /** When a node becomes leader, it starts sending heartbeats */
    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }

    /** Sends heartbeats to all peers via HTTP */
    private void sendHeartbeats() {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (state.getRole() == Role.LEADER) {
                    for (String peerUrl : peerUrls) {
                        sendHeartbeat(peerUrl);
                    }
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    /** Sends a single heartbeat to a given peer */
    private void sendHeartbeat(String peerUrl) {
        try {
            String url = peerUrl + "/raft/heartbeat"; // e.g., "http://node2:8080/raft/heartbeat"
            HeartbeatDTO heartbeat = new HeartbeatDTO(state.getCurrentTerm(), state.getNodeId());
            restTemplate.postForEntity(url, heartbeat, Void.class);
        } catch (Exception e) {
            System.err.println("❌ Error sending heartbeat to " + peerUrl + ": " + e.getMessage());
        }
    }

    /** When a heartbeat is received, a follower resets its election timer */
    public synchronized void receiveHeartbeat(int term) {
        if (term >= state.getCurrentTerm()) {
            state.setRole(Role.FOLLOWER);
            state.setCurrentTerm(term);
            resetElectionTimer();
            System.out.println("💓 Node " + state.getNodeId() + " received heartbeat for term " + term);
        }
    }

    /** Retries the election if a majority of votes is not reached */
    private void retryElection() {
        int retryTimeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        System.out.println("🔄 Node " + state.getNodeId() + " retrying election in " + retryTimeout + "ms");
        scheduler.schedule(new Runnable() {
            @Override
            public void run() {
                startElection();
            }
        }, retryTimeout, TimeUnit.MILLISECONDS);
    }
}
