import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Service
public class RaftLogManager {
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftNodeState raftNodeState;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.raftNodeState = raftNode.getState();
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void initialize() {
        if (raftNodeState.getRole() == Role.LEADER) {
            initializeIndices();
        }
    }

    private void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNode.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        int currentTerm = raftNodeState.getCurrentTerm();
        int leaderTerm = dto.getTerm();
        int prevLogIndex = dto.getPrevLogIndex();
        int prevLogTerm = dto.getPrevLogTerm();
        List<LogEntry> entries = dto.getEntries();
        int leaderCommit = dto.getLeaderCommit();

        if (leaderTerm < currentTerm) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        if (leaderTerm > currentTerm) {
            raftNodeState.setCurrentTerm(leaderTerm);
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            raftNode.resetElectionTimer();
            currentTerm = leaderTerm;
        }

        if (prevLogIndex > 0 && (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        int index = prevLogIndex + 1;
        for (LogEntry entry : entries) {
            if (raftLog.containsEntryAt(index) && raftLog.getTermAt(index) != entry.getTerm()) {
                raftLog.deleteFrom(index);
            }
            if (!raftLog.containsEntryAt(index)) {
                raftLog.append(entry);
            }
            index++;
        }

        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
        }

        raftNode.resetElectionTimer();
        return new AppendEntryResponseDTO(currentTerm, true);
    }

    /**
     * Replicates new log entries and waits for majority commit.
     * Returns a future that completes when the entry is committed.
     */
    
    public CompletableFuture<Void> replicateLogToFollowers(List<LogEntry> newEntries) {
        if (raftNodeState.getRole() != Role.LEADER) {
            return CompletableFuture.failedFuture(new IllegalStateException("Not leader"));
        }
    
        if (newEntries == null || newEntries.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
    
        int startIndex = raftLog.getLastIndex() + 1;
        newEntries.forEach(raftLog::append);
        int finalIndexOfNewEntries = raftLog.getLastIndex();  // after appending
        int currentTerm = raftNodeState.getCurrentTerm();
    
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }
    
        // For each follower, replicate asynchronously (possibly re-trying) ...
        @SuppressWarnings("unchecked")
        CompletableFuture<Boolean>[] futures = raftNode.getPeerUrls().stream()
                .map(peerUrl -> replicateToFollower(peerUrl, currentTerm, finalIndexOfNewEntries, 500))
                .toArray(CompletableFuture[]::new);
    
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
    
        // After we gather all results (or time out), we compute how many succeeded
        CompletableFuture<Void> majorityFuture = CompletableFuture.allOf(futures)
            .thenRunAsync(() -> {
                // Count how many actually returned true
                int successes = 1; // count leader itself
                for (CompletableFuture<Boolean> f : futures) {
                    try {
                        if (f.get()) {
                            successes++;
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        // ignore
                    }
                }
                // If we don't have a majority at all, you can fail or just skip
                if (successes < majority) {
                    throw new RuntimeException("Failed to achieve majority commit");
                }
    
                // *** Correctly advance commitIndex as per the Raft paper ***
                int newCommitIndex = raftLog.getCommitIndex();
                int lastIndex = raftLog.getLastIndex();
    
                for (int i = newCommitIndex + 1; i <= lastIndex; i++) {
                    if (raftLog.getTermAt(i) == currentTerm) {
                        // Count how many matchIndex >= i
                        int count = 1; // leader
                        for (String peer : matchIndex.keySet()) {
                            if (matchIndex.get(peer) >= i) {
                                count++;
                            }
                        }
                        if (count >= majority) {
                            newCommitIndex = i;
                        }
                    }
                }
    
                // Now set commitIndex
                if (newCommitIndex > raftLog.getCommitIndex()) {
                    raftLog.setCommitIndex(newCommitIndex);
                }
            }, raftNode.getAsyncExecutor());
    
        return majorityFuture;
    }
    
    private CompletableFuture<Void> replicateToFollower(
            String peerUrl, int currentTerm, int targetIndex, long timeoutMs) {
    
        // The core replication logic remains the same:
        CompletableFuture<Void> replicationFuture = CompletableFuture.runAsync(() -> {
            synchronized (this) {
                if (raftNodeState.getRole() != Role.LEADER) return;
    
                int ni = nextIndex.get(peerUrl);
                int prevLogIndex = Math.max(0, ni - 1);
                int prevLogTerm = (prevLogIndex > 0) ? raftLog.getTermAt(prevLogIndex) : 0;
                List<LogEntry> entriesToSend = getEntriesFrom(ni, targetIndex);
    
                // Build AppendEntry
                AppendEntryDTO dto = new AppendEntryDTO(
                    currentTerm,
                    raftNodeState.getNodeId(),
                    prevLogIndex,
                    prevLogTerm,
                    entriesToSend,
                    raftLog.getCommitIndex()
                );
    
                // Send request (blocking this thread until done)
                AppendEntryResponseDTO response = sendAppendEntriesAsync(peerUrl, dto, 500).join();
    
                // Compare terms
                if (response.getTerm() > currentTerm) {
                    // Step down if higher term found
                    raftNodeState.setCurrentTerm(response.getTerm());
                    raftNodeState.setRole(Role.FOLLOWER);
                    raftNodeState.setVotedFor(null);
                    raftNode.resetElectionTimer();
                    return;
                }
    
                // If success, advance matchIndex / nextIndex
                if (response.isSuccess()) {
                    nextIndex.put(peerUrl, ni + entriesToSend.size());
                    matchIndex.put(peerUrl, ni + entriesToSend.size() - 1);
                } else {
                    // Conflict => decrement nextIndex by 1
                    ni = Math.max(1, ni - 1);
                    nextIndex.put(peerUrl, ni);
                }
            }
        }, raftNode.getAsyncExecutor());
    
        // Attach a timeout to the replication future:
        // If the future does not complete within 'timeoutMs', it completes exceptionally.
        return replicationFuture.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private CompletableFuture<AppendEntryResponseDTO> sendAppendEntriesAsync(String peerUrl, AppendEntryDTO dto, long timeoutMs) {
        CompletableFuture<AppendEntryResponseDTO> future = CompletableFuture.supplyAsync(() -> {
            try {
                String url = peerUrl + "/raft/appendEntries";
                ResponseEntity<AppendEntryResponseDTO> response = raftNode.getRestTemplate()
                    .postForEntity(url, dto, AppendEntryResponseDTO.class);
    
                if (response.getBody() != null) {
                    return response.getBody();
                } else {
                    return new AppendEntryResponseDTO(-1, false);
                }
            } catch (Exception e) {
                return new AppendEntryResponseDTO(-1, false);
            }
        }, raftNode.getAsyncExecutor());
    
        return future.orTimeout(timeoutMs, TimeUnit.MILLISECONDS);
    }


    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
            entries.add(raftLog.getEntryAt(i));
        }
        return entries;
    }
}
