import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RaftLogManager {
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftNodeState raftNodeState;
    private final Map<String, Integer> nextIndex = raftNodeState.nextIndex;
    private final Map<String, Integer> matchIndex = raftNodeState.matchIndex;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        initializeIndices();
    }

    /**
     * Initializes nextIndex and matchIndex for each peer when the node becomes a leader.
     */
    private void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNode.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    /**
     * Handles incoming AppendEntries RPCs from the leader.
     * Returns the current term and success status, plus the last matching index if failed.
     * Raft paper Section 5.3.
     */
    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        int currentTerm = raftNode.getState().getCurrentTerm();
        int leaderTerm = dto.getTerm();
        int prevLogIndex = dto.getPrevLogIndex();
        int prevLogTerm = dto.getPrevLogTerm();
        List<LogEntry> entries = dto.getEntries();
        int leaderCommit = dto.getLeaderCommit();

        // 1. Reject if leader's term is less than current term
        if (leaderTerm < currentTerm) {
            return new AppendEntryResponseDTO(currentTerm, false, raftLog.getLastIndex());
        }

        // 2. Update term and step down if leader's term is higher
        if (leaderTerm > currentTerm) {
            raftNode.getState().setCurrentTerm(leaderTerm);
            raftNode.getState().setRole(Role.FOLLOWER);
            raftNode.getState().setVotedFor(null);
            raftNode.resetElectionTimer();
        }

        // 3. Check log consistency at prevLogIndex
        if (prevLogIndex > 0 && (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            int lastMatchingIndex = findLastMatchingIndex(prevLogIndex);
            return new AppendEntryResponseDTO(currentTerm, false, lastMatchingIndex);
        }

        // 4. Resolve conflicts and append entries
        int index = prevLogIndex + 1;
        for (LogEntry entry : entries) {
            if (raftLog.containsEntryAt(index)) {
                if (raftLog.getTermAt(index) != entry.getTerm()) {
                    raftLog.deleteFrom(index);
                    raftLog.append(entry);
                }
            } else {
                raftLog.append(entry);
            }
            index++;
        }

        // 5. Update commit index
        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
        }

        return new AppendEntryResponseDTO(currentTerm, true, raftLog.getLastIndex());
    }

    /**
     * Replicates a log entry to followers, adjusting nextIndex on mismatch and retrying.
     * Raft paper Section 5.3.
     */
    public void replicateLogEntryToFollowers(LogEntry entry) {
        if (raftNode.getState().getRole() != Role.LEADER) {
            return;
        }

        // Append to leader's log first
        raftLog.append(entry);
        int currentTerm = raftNode.getState().getCurrentTerm();
        int lastLogIndex = raftLog.getLastIndex();

        // Update nextIndex for new followers or reset on leader election
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }

        // Send AppendEntries RPCs to all followers
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String peerUrl : raftNode.getPeerUrls()) {
            futures.add(replicateToFollower(peerUrl, currentTerm, lastLogIndex));
        }

        // Process replication results and commit if majority achieved
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenRun(() -> {
            int replicatedCount = 0;
            for (String peerUrl : raftNode.getPeerUrls()) {
                if (matchIndex.getOrDefault(peerUrl, 0) >= lastLogIndex) {
                    replicatedCount++;
                }
            }
            if (replicatedCount >= raftNode.getPeerUrls().size() / 2) {
                raftLog.setCommitIndex(lastLogIndex);
            }
        });
    }

    /**
     * Replicates log entries to a single follower, adjusting nextIndex on mismatch.
     */
    private CompletableFuture<Void> replicateToFollower(String peerUrl, int currentTerm, int targetIndex) {
        return CompletableFuture.runAsync(() -> {
            while (raftNode.getState().getRole() == Role.LEADER && nextIndex.get(peerUrl) <= targetIndex) {
                int ni = nextIndex.get(peerUrl);
                int prevLogIndex = ni - 1;
                int prevLogTerm = raftLog.getTermAt(prevLogIndex);
                List<LogEntry> entriesToSend = getEntriesFrom(ni, targetIndex);

                AppendEntryDTO dto = new AppendEntryDTO(
                    currentTerm,
                    raftNode.getState().getNodeId(),
                    prevLogIndex,
                    prevLogTerm,
                    entriesToSend,
                    raftLog.getCommitIndex()
                );

                AppendEntryResponseDTO response = sendAppendEntriesAsync(peerUrl, dto).join();
                synchronized (this) {
                    if (response.getTerm() > currentTerm) {
                        raftNode.getState().setCurrentTerm(response.getTerm());
                        raftNode.getState().setRole(Role.FOLLOWER);
                        raftNode.getState().setVotedFor(null);
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
            }
        }, raftNode.getAsyncExecutor());
    }

    /**
     * Sends an AppendEntries RPC to a peer asynchronously.
     */
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

    /**
     * Finds the last index where the log matches the leader's prevLogIndex and prevLogTerm.
     */
    private int findLastMatchingIndex(int prevLogIndex) {
        for (int i = Math.min(prevLogIndex, raftLog.getLastIndex()); i >= 1; i--) {
            if (raftLog.containsEntryAt(i)) {
                return i;
            }
        }
        return 0;
    }

    /**
     * Retrieves log entries from startIndex to endIndex (inclusive).
     */
    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
            entries.add(raftLog.getEntryAt(i));
        }
        return entries;
    }
}
