import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ElectionManager {
    private final RaftConfig raftConfig;
    private final RaftLog raftLog;
    private final RaftNodeState nodeState;
    private final RestTemplate restTemplate;

    public synchronized VoteResponseDTO handleVoteRequest(RequestVoteDTO requestVote) {
        int currentTerm = nodeState.getCurrentTerm();
        int requestTerm = requestVote.getTerm();
        int candidateId = requestVote.getCandidateId();
        int candidateLastTerm = requestVote.getLastLogTerm();
        int candidateLastIndex = requestVote.getLastLogIndex();

        if (requestTerm < currentTerm) {
            return new VoteResponseDTO(currentTerm, false);
        }

        if (requestTerm > currentTerm) {
            raftNode.becomeFollower(requestTerm);
            currentTerm = requestTerm; 
        }

        Integer votedFor = nodeState.getVotedFor();
        if (votedFor != null && !votedFor.equals(candidateId)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        int localLastTerm = raftLog.getLastTerm();
        int localLastIndex = raftLog.getLastIndex();
        if (candidateLastTerm < localLastTerm ||
            (candidateLastTerm == localLastTerm && candidateLastIndex < localLastIndex)) {
            return new VoteResponseDTO(currentTerm, false);
        }

        nodeState.setVotedFor(candidateId); 
        raftNode.resetElectionTimer();
        return new VoteResponseDTO(currentTerm, true);
    }

    private void startElection() {
        synchronized (this) {
            if (nodeState.getRole() == Role.LEADER) return;
    
            nodeState.setCurrentRole(Role.CANDIDATE);
            nodeState.incrementTerm();
            nodeState.setVotedFor(nodeState.getNodeId());
    
            int currentTerm = nodeState.getCurrentTerm();
            List<CompletableFuture<VoteResponseDTO>> voteFutures = new ArrayList<>();
            ExecutorService executor = raftNode.getAsyncExecutor();
    
            for (String peerUrl : nodeState.getPeerUrls()) {
                CompletableFuture<VoteResponseDTO> voteFuture = CompletableFuture
                    .supplyAsync(() -> requestVote(
                        currentTerm,
                        nodeState.getNodeId(),
                        raftLog.getLastIndex(),
                        raftLog.getLastTerm(),
                        peerUrl
                    ), executor)
                    .orTimeout(raftConfig.getElectionRpcTimeoutMillis(), TimeUnit.MILLISECONDS)
                    .exceptionally(throwable -> new VoteResponseDTO(currentTerm, false));
                voteFutures.add(voteFuture);
            }
    
            int majority = (nodeState.getPeerUrls().size() + 1) / 2 + 1;
            AtomicInteger voteCount = new AtomicInteger(1); // Self-vote
    
            for (CompletableFuture<VoteResponseDTO> future : voteFutures) {
                future.thenAccept(response -> {
                    synchronized (this) {
                        if (nodeState.getRole() != Role.CANDIDATE || nodeState.getCurrentTerm() != currentTerm) {
                            return;
                        }
                        if (response != null && response.isVoteGranted()) {
                            if (voteCount.incrementAndGet() >= majority) {
                                raftNode.becomeLeader();
                            }
                        }
                    }
                });
            }
            
        raftNode.resetElectionTimer();
    }

    private VoteResponseDTO requestVote(int term, int candidateId, int lastLogIndex, int lastLogTerm, String peerUrl) {
        try {
            String url = peerUrl + "/raft/requestVote";
            RequestVoteDTO dto = new RequestVoteDTO(term, candidateId, lastLogIndex, lastLogTerm);
            ResponseEntity<VoteResponseDTO> response = restTemplate.postForEntity(url, dto, VoteResponseDTO.class);
            VoteResponseDTO body = response.getBody() != null ? response.getBody() : new VoteResponseDTO(term, false);

            if (body.getTerm() > nodeState.getCurrentTerm()) {
                raftNode.becomeFollower(body.getTerm());
            }
            return body;
        } catch (Exception e) {
            return new VoteResponseDTO(term, false);
        }
    }
}
