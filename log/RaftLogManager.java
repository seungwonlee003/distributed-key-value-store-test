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
    private final ScheduledExecutorService replicationExecutor;
    private final Map<String, Boolean> pendingReplication = new ConcurrentHashMap<>();

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.replicationExecutor = Executors.newScheduledThreadPool(raftNode.getPeerUrls().size());
    }

    public void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNode.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    public boolean handleClientRequest(LogEntry clientEntry) {
        raftLog.append(clientEntry);
        int entryIndex = raftLog.getLastIndex();
    
        long start = System.currentTimeMillis();
        long timeoutMillis = 3000; // timeout to prevent infinite loop
    
        while (raftNode.getRole() == Role.LEADER) {
            if (raftLog.getCommitIndex() >= entryIndex) {
                return true;
            }
            if (System.currentTimeMillis() - start > timeoutMillis) {
                return false; // Avoid infinite loop
            }
    
            try {
                Thread.sleep(1000); // Polling interval
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    
        return false; // Leadership lost
    }
    
    public void startLogReplication() {
        if (raftNode.getRole() != Role.LEADER) return;

        for (String peerUrl : raftNode.getPeerUrls()) {
            if (!pendingReplication.getOrDefault(peerUrl, false)) {
                pendingReplication.put(peerUrl, true);
                replicationExecutor.submit(() -> replicateToFollowerLoop(peerUrl));
            }
        }
    }
        
    private void replicateToFollowerLoop(String peerUrl) {
        // the frequency of heartbeats depends on replicateToFollowerLoop
        int backoffMs = 1000;
        while (raftNode.getRole() == Role.LEADER) {
            boolean success = replicateToFollower(peerUrl);
            if (success) {
                backoffMs = 1000;
                updateCommitIndex(); 
            } else {
                backoffMs = Math.min(backoffMs * 2, 5000);
            }

            try {
                Thread.sleep(backoffMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        pendingReplication.put(peerUrl, false);
    }

    private boolean replicateToFollower(String peerUrl) {
        if (raftNode.getRole() != Role.LEADER) return false;

        int ni = nextIndex.get(peerUrl);
        int prevLogIndex = ni - 1;
        int prevLogTerm = (prevLogIndex >= 0) ? raftLog.getTermAt(prevLogIndex) : 0;
        List<LogEntry> entries = raftLog.getEntriesFrom(ni);

        AppendEntryDTO dto = new AppendEntryDTO(
            raftNode.getCurrentTerm(),
            raftNode.getNodeId(),
            prevLogIndex,
            prevLogTerm,
            entries,
            raftLog.getCommitIndex()
        );

        try {
            AppendEntryResponseDTO response = sendAppendEntries(peerUrl, dto);
            if (response.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.becomeFollower(response.getTerm());
                return false;
            }

            if (response.isSuccess()) {
                nextIndex.put(peerUrl, ni + entries.size());
                matchIndex.put(peerUrl, ni + entries.size() - 1);
                return true;
            } else {
                nextIndex.put(peerUrl, Math.max(0, ni - 1)); // Backtrack
                return false;
            }
        } catch (Exception e) {
            return false; // Retry on network failure
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

    private void updateCommitIndex() {
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        int currentTerm = raftNode.getCurrentTerm();
    
        // Find the largest N where majority of matchIndex >= N
        for (int i = raftLog.getLastIndex(); i > raftLog.getCommitIndex(); i--) {
            int count = 1; // Leader
            for (int index : matchIndex.values()) {
                if (index >= i) count++;
            }
            if (count >= majority && raftLog.getTermAt(i) == currentTerm) {
                raftLog.setCommitIndex(i);
                applyCommittedEntries();
                break;
            }
        }
    }

    // make it async
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
                System.exit(1); // FIX: #7 (prevent gap in lastApplied) - make it system.exit(1)?
            }
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
}
