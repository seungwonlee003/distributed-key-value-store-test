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
        int targetIndex = raftLog.getLastIndex();

        int currentTerm = raftNodeState.getCurrentTerm();
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }

        CompletableFuture<?>[] futures = raftNode.getPeerUrls().stream()
            .map(peerUrl -> replicateToFollower(peerUrl, currentTerm, targetIndex))
            .toArray(CompletableFuture[]::new);

        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        CompletableFuture<Void> majorityFuture = CompletableFuture.runAsync(() -> {
            synchronized (this) {
                int successes = 1; // Leader counts as 1
                for (int i = 0; i < futures.length; i++) {
                    try {
                        futures[i].join();
                        String peerUrl = raftNode.getPeerUrls().get(i);
                        if (matchIndex.getOrDefault(peerUrl, 0) >= targetIndex) {
                            successes++;
                        }
                    } catch (Exception e) {
                        // Follower failed
                    }
                    if (successes >= majority) {
                        updateCommitIndex(currentTerm);
                        break;
                    }
                }
                if (successes < majority) {
                    throw new RuntimeException("Failed to achieve majority commit");
                }
            }
        }, raftNode.getAsyncExecutor());

        return majorityFuture.thenCompose(v -> waitForCommit(targetIndex));
    }



    /**
     * Waits until the commit index reaches or exceeds the target index.
     */
    public CompletableFuture<Void> waitForCommit(int targetIndex) {
        if (raftLog.getCommitIndex() >= targetIndex) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        Consumer<Integer> listener = new Consumer<>() {
            @Override
            public void accept(Integer newIndex) {
                if (newIndex >= targetIndex) {
                    future.complete(null);
                    raftLog.removeCommitIndexListener(this);
                }
            }
        };
        raftLog.addCommitIndexListener(listener);
        return future.orTimeout(10, TimeUnit.SECONDS);
    }

    private void updateCommitIndex(int currentTerm) {
        int currentCommitIndex = raftLog.getCommitIndex();
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        List<Integer> matchIndices = new ArrayList<>();
        matchIndices.add(raftLog.getLastIndex()); // Leader’s log
        for (String peerUrl : raftNode.getPeerUrls()) {
            matchIndices.add(matchIndex.getOrDefault(peerUrl, 0));
        }

        for (int N = raftLog.getLastIndex(); N > currentCommitIndex; N--) {
            if (raftLog.getTermAt(N) == currentTerm) {
                int count = (int) matchIndices.stream().filter(idx -> idx >= N).count();
                if (count >= majority) {
                    raftLog.setCommitIndex(N);
                    break;
                }
            }
        }
    }

    private CompletableFuture<Void> replicateToFollower(String peerUrl, int currentTerm, int targetIndex) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this) {
                if (raftNodeState.getRole() != Role.LEADER) return;

                int ni = nextIndex.get(peerUrl);
                int prevLogIndex = Math.max(0, ni - 1);
                int prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;
                List<LogEntry> entriesToSend = getEntriesFrom(ni, targetIndex);

                AppendEntryDTO dto = new AppendEntryDTO(
                    currentTerm, raftNodeState.getNodeId(), prevLogIndex, prevLogTerm,
                    entriesToSend, raftLog.getCommitIndex()
                );

                AppendEntryResponseDTO response = sendAppendEntriesAsync(peerUrl, dto).join();
                if (response.getTerm() > currentTerm) {
                    raftNodeState.setCurrentTerm(response.getTerm());
                    raftNodeState.setRole(Role.FOLLOWER);
                    raftNodeState.setVotedFor(null);
                    raftNode.resetElectionTimer();
                    return;
                }
                if (response.isSuccess()) {
                    nextIndex.put(peerUrl, ni + entriesToSend.size());
                    matchIndex.put(peerUrl, ni + entriesToSend.size() - 1);
                } else {
                    ni = Math.max(1, ni - 1); // Decrement by 1
                    nextIndex.put(peerUrl, ni);
                }
            }
        }, raftNode.getAsyncExecutor());
    }

    private CompletableFuture<AppendEntryResponseDTO> sendAppendEntriesAsync(String peerUrl, AppendEntryDTO dto) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = peerUrl + "/raft/appendEntries";
                ResponseEntity<AppendEntryResponseDTO> response = raftNode.getRestTemplate()
                    .postForEntity(url, dto, AppendEntryResponseDTO.class);
                return response.getBody() != null ? response.getBody() : new AppendEntryResponseDTO(-1, false);
            } catch (Exception e) {
                return new AppendEntryResponseDTO(-1, false);
            }
        }, raftNode.getAsyncExecutor());
    }

    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
            entries.add(raftLog.getEntryAt(i));
        }
        return entries;
    }
}
