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
    private final StateMachine stateMachine;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.stateMachine = raftNode.getStateMachine(); // Use facade to get state machine
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
        for (LogEntry entry : entries) {
            if (raftLog.containsEntryAt(index) && raftLog.getTermAt(index) != entry.getTerm()) {
                raftLog.deleteFrom(index);
            }
            if (!raftLog.containsEntryAt(index)) {
                raftLog.append(entry);
            }
            index++;
        }
    }

    public synchronized void replicateLogToFollowers(List<LogEntry> newEntries) throws Exception {
        if (raftNode.getRole() != Role.LEADER) {
            throw new IllegalStateException("Not leader");
        }

        if (newEntries != null && !newEntries.isEmpty()) {
            newEntries.forEach(raftLog::append);
        }

        int finalIndexOfNewEntries = raftLog.getLastIndex();
        int currentTerm = raftNode.getCurrentTerm();
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }

        ExecutorService executor = raftNode.getAsyncExecutor();
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;

        AtomicInteger successes = new AtomicInteger(1); // Leader counts itself
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
        int newCommitIndex = raftLog.getCommitIndex();
        int currentTerm = raftNode.getCurrentTerm();

        for (int i = newCommitIndex + 1; i <= finalIndexOfNewEntries; i++) {
            if (raftLog.getTermAt(i) == currentTerm) {
                int count = 1; // Leader
                for (String peer : matchIndex.keySet()) {
                    if (matchIndex.get(peer) >= i) count++;
                }
                if (count >= majority) newCommitIndex = i;
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
            LogEntry entry = raftLog.getEntryAt(i);
            raftNode.getStateMachine().apply(entry);
            raftNode.setLastApplied(i);
        }
    }
}
