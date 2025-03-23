import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ElectionManager {
    private final RaftNode raftNode;

    public ElectionManager(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        int currentTerm = raftNode.getCurrentTerm();
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        if (requestTerm < currentTerm) {
            return new VoteResponseDTO(currentTerm, false);
        }

        if (requestTerm > currentTerm) {
            raftNode.becomeFollower(requestTerm);
            currentTerm = requestTerm; // Update local term after facade call
        }

        Integer votedFor = raftNode.getState().getVotedFor(); // Minimal state access for votedFor
        if (votedFor != null && !votedFor.equals(candidateId)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        // Retrieve log details via RaftNode facade
        int localLastTerm = raftNode.getRaftLog().getLastTerm();
        int localLastIndex = raftNode.getRaftLog().getLastIndex();
        if (candidateLastTerm < localLastTerm ||
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        raftNode.getState().setVotedFor(candidateId); // Direct state access here, see note below
        raftNode.resetElectionTimer();
        return new VoteResponseDTO(currentTerm, true);
    }

    private void startElection() {
        synchronized (this) {
            if (raftNode.getRole() == Role.LEADER) return;

            // Transition to candidate and prepare for election
            raftNode.getState().setRole(Role.CANDIDATE);
            raftNode.getState().incrementTerm();
            raftNode.getState().setVotedFor(raftNode.getNodeId());

            int currentTerm = raftNode.getCurrentTerm();
            List<CompletableFuture<VoteResponseDTO>> voteFutures = new ArrayList<>();
            ExecutorService executor = raftNode.getAsyncExecutor();

            for (String peerUrl : raftNode.getPeerUrls()) {
                CompletableFuture<VoteResponseDTO> voteFuture = CompletableFuture
                    .supplyAsync(() -> requestVote(
                        currentTerm,
                        raftNode.getNodeId(),
                        raftNode.getRaftLog().getLastIndex(),
                        raftNode.getRaftLog().getLastTerm(),
                        peerUrl
                    ), executor)
                    .orTimeout(1000, TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> new VoteResponseDTO(currentTerm, false));
                voteFutures.add(voteFuture);
            }

            CompletableFuture.allOf(voteFutures.toArray(new CompletableFuture[0])).thenRun(() -> {
                synchronized (this) {
                    if (raftNode.getRole() != Role.CANDIDATE || raftNode.getCurrentTerm() != currentTerm) {
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
                        raftNode.becomeLeader();
                    } else {
                        raftNode.resetElectionTimer();
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

            if (body.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.becomeFollower(body.getTerm());
            }
            return body;
        } catch (Exception e) {
            return new VoteResponseDTO(term, false);
        }
    }
}
