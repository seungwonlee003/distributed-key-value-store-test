import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
public class RaftLogManager {
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
    }

    public void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNode.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        int currentTerm = raftNode.getCurrentTerm();
        int leaderTerm = dto.getTerm();
        int prevLogIndex = dto.getPrevLogIndex();
        int prevLogTerm = dto.getPrevLogTerm();
        List<LogEntry> entries = dto.getEntries();
        int leaderCommit = dto.getLeaderCommit();

        if (leaderTerm < currentTerm) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        if (leaderTerm > currentTerm) {
            raftNode.becomeFollower(leaderTerm);
            currentTerm = leaderTerm;
        }

        if (prevLogIndex > 0 &&
            (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        appendEntries(prevLogIndex, entries);

        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
            applyCommittedEntries();
        }

        raftNode.resetElectionTimer();
        return new AppendEntryResponseDTO(currentTerm, true);
    }

    private void appendEntries(int prevLogIndex, List<LogEntry> entries) {
        int index = prevLogIndex + 1;
        if (!entries.isEmpty()) {
            if (raftLog.containsEntryAt(index) && raftLog.getTermAt(index) != entries.get(0).getTerm()) {
                raftLog.deleteFrom(index);
            }
            raftLog.appendAll(entries);
        }
    }

    public synchronized void replicateLogToFollowers(List<LogEntry> newEntries) throws Exception {
        // stop and heartbeat and start again at the end? because two threads could interfere.
        if (raftNode.getRole() != Role.LEADER) {
            throw new IllegalStateException("Not leader");
        }

        if (newEntries != null && !newEntries.isEmpty()) {
            raftLog.appendAll(newEntries);
        }
        
        int finalIndexOfNewEntries = raftLog.getLastIndex();
        int currentTerm = raftNode.getCurrentTerm();
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }

        ExecutorService executor = raftNode.getAsyncExecutor();
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;

        AtomicInteger successes = new AtomicInteger(1);
        CountDownLatch majorityLatch = new CountDownLatch(majority - 1);

        List<CompletableFuture<Void>> futures = raftNode.getPeerUrls().stream()
            .map(peerUrl -> CompletableFuture.runAsync(() -> {
                boolean success = replicateToFollower(peerUrl, currentTerm, finalIndexOfNewEntries);
                if (success && successes.incrementAndGet() <= majority) {
                    majorityLatch.countDown();
                }
            }, executor)
            .exceptionally(throwable -> null))
            .collect(Collectors.toList());

        boolean majorityAchieved = majorityLatch.await(1000, TimeUnit.MILLISECONDS);

        if (!majorityAchieved || successes.get() < majority) {
            throw new RuntimeException("Failed to achieve majority commit");
        }

        commitLogEntries(finalIndexOfNewEntries);
    }

    private void commitLogEntries(int finalIndexOfNewEntries) {
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        int currentTerm = raftNode.getCurrentTerm();
        int newCommitIndex = raftLog.getCommitIndex();
    
        // Track the highest index replicated to a majority (any term)
        int maxReplicated = raftLog.getCommitIndex();
        for (int peerIndex : matchIndex.values()) {
            if (peerIndex > maxReplicated) maxReplicated = peerIndex;
        }
    
        // Find the largest N where N > commitIndex, and a majority of matchIndex[i] ≥ N
        for (int i = maxReplicated; i > raftLog.getCommitIndex(); i--) {
            int count = 1; // Leader
            for (int peerIndex : matchIndex.values()) {
                if (peerIndex >= i) count++;
            }
            if (count >= majority) {
                // Ensure at least one entry in the current term is included (Raft §5.4.2)
                if (raftLog.getTermAt(i) == currentTerm) {
                    newCommitIndex = i;
                    break;
                }
            }
        }
    
        if (newCommitIndex > raftLog.getCommitIndex()) {
            raftLog.setCommitIndex(newCommitIndex);
            applyCommittedEntries();
        }
    }
    
    private boolean replicateToFollower(String peerUrl, int currentTerm, int targetIndex) {
        if (raftNode.getRole() != Role.LEADER) {
            return false;
        }

        int ni = nextIndex.get(peerUrl);
        int prevLogIndex = Math.max(0, ni - 1);
        int prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;
        List<LogEntry> entriesToSend = raftLog.getEntriesFrom(ni, targetIndex);

        AppendEntryDTO dto = new AppendEntryDTO(
            currentTerm,
            raftNode.getNodeId(),
            prevLogIndex,
            prevLogTerm,
            entriesToSend,
            raftLog.getCommitIndex()
        );

        AppendEntryResponseDTO response = sendAppendEntries(peerUrl, dto);

        if (response.getTerm() > currentTerm) {
            raftNode.becomeFollower(response.getTerm());
            return false;
        }

        if (response.isSuccess()) {
            nextIndex.put(peerUrl, ni + entriesToSend.size());
            matchIndex.put(peerUrl, ni + entriesToSend.size() - 1);
            return true;
        } else {
            int backtrack = Math.max(0, ni - 1);
            nextIndex.put(peerUrl, backtrack);
            return false;
        }
    }

    private AppendEntryResponseDTO sendAppendEntries(String peerUrl, AppendEntryDTO dto) {
        try {
            String url = peerUrl + "/raft/appendEntries";
            ResponseEntity<AppendEntryResponseDTO> response =
                raftNode.getRestTemplate().postForEntity(url, dto, AppendEntryResponseDTO.class);
            return response.getBody() != null ? response.getBody() : new AppendEntryResponseDTO(-1, false);
        } catch (Exception e) {
            return new AppendEntryResponseDTO(-1, false);
        }
    }

    private void applyCommittedEntries() {
        int commitIndex = raftLog.getCommitIndex();
        int lastApplied = raftNode.getLastApplied();
        for (int i = lastApplied + 1; i <= commitIndex; i++) {
           try {
                LogEntry entry = raftLog.getEntryAt(i);
                raftNode.getStateMachine().apply(entry);
                raftNode.setLastApplied(i);
            } catch (Exception e) {
                System.out.println("State machine apply failed at index " + i + ": " + e.getMessage());
                break; // FIX: #7 (prevent gap in lastApplied)
            }
        }
    }
}
