import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class ElectionManager {
    private final RaftNode raftNode;

    public ElectionManager(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        RaftNodeState state = raftNode.getState();
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
            raftNode.becomeFollower(requestTerm);
            currentTerm = requestTerm;
        }

        if (votedFor != null && !votedFor.equals(candidateId)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // Retrieve log details via the RaftNode facade.
        int localLastTerm = raftNode.getRaftLog().getLastTerm();
        int localLastIndex = raftNode.getRaftLog().getLastIndex();
        if (candidateLastTerm < localLastTerm ||
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        state.setVotedFor(candidateId);
        resetElectionTimer();
        return new VoteResponseDTO(currentTerm, true);
    }

    private void startElection() {
        synchronized (this) {
            RaftNodeState state = raftNode.getState();
            if (state.getRole() == Role.LEADER) return;

            state.setRole(Role.CANDIDATE);
            state.incrementTerm();
            state.setVotedFor(state.getNodeId());

            int currentTerm = state.getCurrentTerm();
            List<CompletableFuture<VoteResponseDTO>> voteFutures = new ArrayList<>();
            ExecutorService executor = raftNode.getAsyncExecutor();

            for (String peerUrl : raftNode.getPeerUrls()) {
                CompletableFuture<VoteResponseDTO> voteFuture = CompletableFuture
                    .supplyAsync(() -> requestVote(currentTerm, state.getNodeId(), 
                                                   raftNode.getRaftLog().getLastIndex(), raftNode.getRaftLog().getLastTerm(), peerUrl), executor)
                    .orTimeout(1000, TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> new VoteResponseDTO(currentTerm, false));
                voteFutures.add(voteFuture);
            }

            CompletableFuture.allOf(voteFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
                synchronized (this) {
                    if (state.getRole() != Role.CANDIDATE || state.getCurrentTerm() != currentTerm) {
                        return;
                    }
                    int voteCount = 1; // Self-vote
                    for (CompletableFuture<VoteResponseDTO> future : voteFutures) {
                        try {
                            VoteResponseDTO response = future.get();
                            if (response != null && response.isVoteGranted()) {
                                voteCount++;
                            }
                        } catch (Exception e) {
                            // Ignore failures
                        }
                    }
                    int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
                    if (voteCount >= majority) {
                        raftNode.becomeLeader(); // Delegate transition via RaftNode
                    } else {
                        resetElectionTimer();
                    }
                }
            }).exceptionally(ex -> {
                System.err.println("Election failed: " + ex.getMessage());
                return null;
            });
        }
    }

    private VoteResponseDTO requestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm, String peerUrl) {
        try {
            String url = peerUrl + "/raft/requestVote";
            RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
            ResponseEntity<VoteResponseDTO> response = raftNode.getRestTemplate().postForEntity(url, dto, VoteResponseDTO.class);
            VoteResponseDTO body = response.getBody() != null ? response.getBody() : new VoteResponseDTO(term, false);

            synchronized (this) {
                RaftNodeState state = raftNode.getState();
                if (body.getTerm() > state.getCurrentTerm()) {
                    state.setCurrentTerm(body.getTerm());
                    state.setRole(Role.FOLLOWER);
                    state.setVotedFor(null);
                    stopHeartbeats();
                    resetElectionTimer();
                }
            }
            return body;
        } catch (Exception e) {
            return new VoteResponseDTO(term, false);
        }
    }

    public void stopHeartbeats() {
        raftNode.getHeartbeatManager().stopHeartbeats();
    }

    public void resetElectionTimer() {
        raftNode.getElectionTimer().reset();
    }

    public void cancelElectionTimer() {
        raftNode.getElectionTimer().cancel();
    }
}
