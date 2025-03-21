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
            return new AppendEntryResponseDTO(currentTerm, false, raftLog.getLastIndex());
        }

        if (leaderTerm > currentTerm) {
            raftNodeState.setCurrentTerm(leaderTerm);
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            raftNode.resetElectionTimer();
            currentTerm = leaderTerm;
        }

        if (prevLogIndex > 0 && (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            int lastMatchingIndex = findLastMatchingIndex(prevLogIndex);
            return new AppendEntryResponseDTO(currentTerm, false, lastMatchingIndex);
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
        return new AppendEntryResponseDTO(currentTerm, true, raftLog.getLastIndex());
    }

    /** 
     * Replicates log entries to followers. For heartbeats, call with null or empty newEntries.
     * Does not block; use waitForCommit for client writes.
     */
    public void replicateLogToFollowers(List<LogEntry> newEntries) {
        if (raftNodeState.getRole() != Role.LEADER) return;

        if (newEntries != null && !newEntries.isEmpty()) {
            newEntries.forEach(raftLog::append);
        }

        int currentTerm = raftNodeState.getCurrentTerm();
        int lastLogIndex = raftLog.getLastIndex();
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String peerUrl : raftNode.getPeerUrls()) {
            futures.add(replicateToFollower(peerUrl, currentTerm, lastLogIndex));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenRun(() -> {
            synchronized (this) {
                if (raftNodeState.getRole() != Role.LEADER) return;
                updateCommitIndex(currentTerm);
            }
        });
    }

    /** 
     * Waits until the commit index reaches or exceeds the target index.
     * Returns immediately if already committed.
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
        return future;
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
                    int lastMatchingIndex = response.getLastMatchingIndex();
                    nextIndex.put(peerUrl, Math.max(1, lastMatchingIndex + 1));
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
                return response.getBody() != null ? response.getBody() : new AppendEntryResponseDTO(-1, false, -1);
            } catch (Exception e) {
                return new AppendEntryResponseDTO(-1, false, -1);
            }
        }, raftNode.getAsyncExecutor());
    }

    private int findLastMatchingIndex(int prevLogIndex) {
        for (int i = Math.min(prevLogIndex, raftLog.getLastIndex()); i >= 1; i--) {
            if (raftLog.containsEntryAt(i)) {
                return i;
            }
        }
        return 0;
    }

    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
            entries.add(raftLog.getEntryAt(i));
        }
        return entries;
    }
}
