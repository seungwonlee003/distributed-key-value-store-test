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

    // Store the current election timer to allow cancellation
    private ScheduledFuture<?> electionFuture;

    public RaftNode(RaftNodeState state, List<String> peerUrls) {
        this.state = state;
        this.peerUrls = peerUrls;
    }

    @PostConstruct
    public void start() {
        // Raft rules: if follower/candidate, start the timer for an election;
        // if leader, begin heartbeats immediately.
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            resetElectionTimer();
        } else if (state.getRole() == Role.LEADER) {
            sendHeartbeats();
        }
    }

    /**
     * Handle an incoming vote request (the RequestVote RPC in Raft).
     * Returns 'true' if we grant the vote, 'false' otherwise.
     */
    public synchronized boolean handleVoteRequest(RequestVoteDTO requestVote) {
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex= requestVote.getLastLogIndex();

        int currentTerm = state.getCurrentTerm();
        Role currentRole = state.getRole();
        Integer votedFor = state.getVotedFor();

        // 1) If candidate's term < our term => reject
        if (requestTerm < currentTerm) {
            return false;
        }

        // 2) If candidate's term > our currentTerm => step down and update
        if (requestTerm > currentTerm) {
            state.setCurrentTerm(requestTerm);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            cancelElectionTimerIfRunning();   // step down => stop any ongoing election
        }
        // Refresh after possible update
        currentTerm = state.getCurrentTerm();
        currentRole = state.getRole();
        votedFor    = state.getVotedFor();

        // If we are a candidate in the same term as the requester, we must step down as well
        if (requestTerm == currentTerm && currentRole == Role.CANDIDATE) {
            // Another node is also claiming leadership in the same term => become follower
            state.setRole(Role.FOLLOWER);
            cancelElectionTimerIfRunning();
        }

        // 3) If already voted this term for someone else, reject
        if (votedFor != null && !votedFor.equals(candidateId)) {
            return false;
        }

        // 4) Check if candidate’s log is at least as up to date as ours
        int localLastTerm  = state.getLastLogTerm();
        int localLastIndex = state.getLastLogIndex();
        if (candidateLastTerm < localLastTerm) {
            return false;
        }
        if (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex) {
            return false;
        }

        // 5) If all checks pass, grant the vote and reset election timer
        state.setVotedFor(candidateId);
        resetElectionTimer();
        return true;
    }

    /**
     * Send a RequestVote RPC to a peer. If the peer's term is higher, we step down.
     * Returns true if the peer granted our vote, false otherwise.
     */
    public synchronized boolean requestVote(int term, int candidateId,
                                            int lastLogIndex, int lastLogTerm,
                                            String peerUrl) {
        try {
            String url = peerUrl + "/raft/vote";
            RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
            ResponseEntity<VoteResponseDTO> response =
                restTemplate.postForEntity(url, dto, VoteResponseDTO.class);

            VoteResponseDTO body = response.getBody();
            if (body == null) {
                return false;
            }

            int responseTerm = body.getTerm();
            boolean voteGranted = body.isVoteGranted();

            // If we see a strictly higher term, step down
            if (responseTerm > state.getCurrentTerm()) {
                state.setCurrentTerm(responseTerm);
                state.setRole(Role.FOLLOWER);
                state.setVotedFor(null);
                cancelElectionTimerIfRunning();
                return false;
            }
            return voteGranted;
        } catch (Exception e) {
            System.err.println("❌ Error requesting vote from " + peerUrl + ": " + e.getMessage());
            return false;
        }
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
    }

    /**
     * If the timer fires (no heartbeats), we start an election: become CANDIDATE,
     * increment term, vote for self, and request votes from peers.
     */
    private void startElection() {
        if (state.getRole() == Role.FOLLOWER || state.getRole() == Role.CANDIDATE) {
            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());

            int currentTerm = state.getCurrentTerm();
            int votes = 1; // vote for ourselves
            System.out.println("🗳️ Node " + state.getNodeId()
                + " starting election for term " + currentTerm);

            // Ask each peer for a vote
            for (String peerUrl : peerUrls) {
                boolean granted = requestVote(
                    currentTerm,
                    state.getNodeId(),
                    state.getLastLogIndex(),
                    state.getLastLogTerm(),
                    peerUrl
                );
                if (granted) {
                    votes++;
                }
            }

            // Check for majority
            if (votes > peerUrls.size() / 2) {
                becomeLeader();
            } else {
                retryElection();
            }
        }
    }

    /**
     * Transition to LEADER and immediately start sending heartbeats.
     */
    private void becomeLeader() {
        state.setRole(Role.LEADER);
        System.out.println("👑 Node " + state.getNodeId()
            + " became leader for term " + state.getCurrentTerm());
        sendHeartbeats();
    }

    /**
     * Schedules a periodic task to send heartbeats to all peers while we are LEADER.
     */
    private void sendHeartbeats() {
        scheduler.scheduleAtFixedRate(() -> {
            if (state.getRole() == Role.LEADER) {
                for (String peerUrl : peerUrls) {
                    sendHeartbeat(peerUrl);
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Send a single heartbeat (Raft "AppendEntries" with no new log entries) to a peer.
     */
    private void sendHeartbeat(String peerUrl) {
        try {
            String url = peerUrl + "/raft/heartbeat";
            HeartbeatDTO hb = new HeartbeatDTO(state.getCurrentTerm(), state.getNodeId());
            restTemplate.postForEntity(url, hb, Void.class);
        } catch (Exception e) {
            System.err.println("❌ Error sending heartbeat to " + peerUrl + ": " + e.getMessage());
        }
    }

    /**
     * When a heartbeat arrives, if the term is higher, become follower;
     * also if we are a candidate in the same term, we step down.
     */
    public synchronized void receiveHeartbeat(int term) {
        int currentTerm = state.getCurrentTerm();
        if (term > currentTerm) {
            state.setCurrentTerm(term);
            state.setRole(Role.FOLLOWER);
            state.setVotedFor(null);
            resetElectionTimer();
            return;
        }
        // If same term but we’re a candidate => demote to follower
        if (term == currentTerm && state.getRole() == Role.CANDIDATE) {
            state.setRole(Role.FOLLOWER);
        }
        // If follower in the same or higher term, just reset the timer
        resetElectionTimer();
        System.out.println("💓 Node " + state.getNodeId() + " received heartbeat for term " + term);
    }

    /**
     * If we fail to achieve majority, wait a bit and try again if still Follower/Candidate.
     */
    private void retryElection() {
        int retryTimeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        System.out.println("🔄 Node " + state.getNodeId() + " retrying election in " + retryTimeout + "ms");
        scheduler.schedule(this::startElection, retryTimeout, TimeUnit.MILLISECONDS);
    }
}
