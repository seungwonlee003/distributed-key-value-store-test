import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
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

    /**
     * When the node starts, if it's a follower (or candidate), start the election timer.
     * If it's already a leader, start sending heartbeats immediately.
     */
    @PostConstruct
    public void start() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            sendHeartbeats();
        }
    }

    /**
     * Resets the election timer. Cancels any pending election timer task and schedules a new one
     * with a random timeout between electionTimeoutMin and electionTimeoutMax.
     */
    private void resetElectionTimer() {
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

    /**
     * If no heartbeat is received within the election timeout, trigger an election.
     * Transition the node to CANDIDATE, increment the term, vote for itself, and send vote requests.
     */
    private void startElection() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());
            int votes = 1; // Vote for itself

            System.out.println("🗳️ Node " + state.getNodeId() + " starting election for term " + state.getCurrentTerm());

            for (String peerUrl : peerUrls) {
                if (requestVote(state.getCurrentTerm(), state.getNodeId(), state.getLastLogIndex(), state.getLastLogTerm(), peerUrl)) {
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

    /**
     * Sends an HTTP vote request to a peer.
     * Returns true if the peer grants the vote.
     */
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

    /**
     * When a node wins the election, it becomes the leader and starts sending heartbeats.
     */
    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId() + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }

    /**
     * Schedules a periodic task that sends heartbeats to all peers every 100 milliseconds.
     * This task runs only if the node's role is LEADER.
     */
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

    /**
     * Sends a single heartbeat via HTTP to a given peer.
     */
    private void sendHeartbeat(String peerUrl) {
        try {
            String url = peerUrl + "/raft/heartbeat"; // e.g., "http://node2:8080/raft/heartbeat"
            HeartbeatDTO heartbeat = new HeartbeatDTO(state.getCurrentTerm(), state.getNodeId());
            restTemplate.postForEntity(url, heartbeat, Void.class);
        } catch (Exception e) {
            System.err.println("❌ Error sending heartbeat to " + peerUrl + ": " + e.getMessage());
        }
    }

    /**
     * When a heartbeat is received (via HTTP endpoint), a follower resets its election timer.
     */
    public synchronized void receiveHeartbeat(int term) {
        if (term >= state.getCurrentTerm()) {
            state.setRole(Role.FOLLOWER);
            state.setCurrentTerm(term);
            resetElectionTimer();
            System.out.println("💓 Node " + state.getNodeId() + " received heartbeat for term " + term);
        }
    }

    /**
     * If an election fails (i.e. a majority of votes is not reached), schedule a retry
     * with a new randomized timeout.
     */
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
