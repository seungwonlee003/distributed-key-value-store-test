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
    private final RaftConfig raftConfig;
    private final RaftLog raftLog;
    private final RaftStateManager raftStateManager;
    private final RaftNodeState raftNodeState;
    private final RestTemplate restTemplate;
    private final StateMachine stateMachine;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;
    private final ScheduledExecutorService replicationExecutor;
    private final Map<String, Boolean> pendingReplication = new ConcurrentHashMap<>();
    private final ExecutorService applyExecutor = Executors.newSingleThreadExecutor();

    public RaftLogManager() {
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.replicationExecutor = Executors.newScheduledThreadPool(raftNodeState.getPeerUrls().size());
    }

    public void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNodeState.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    public boolean handleClientRequest(LogEntry clientEntry) {
        raftLog.append(clientEntry);
        int entryIndex = raftLog.getLastIndex();
    
        long start = System.currentTimeMillis();
        long timeoutMillis = raftConfig.getClientRequestTimeoutMillis(); 

        // wait for at most 5 seconds for the client's write to be acknowledged by the majority
        while (raftNodeState.getRole() == Role.LEADER) {
            if (raftLog.getCommitIndex() >= entryIndex) {
                return true;
            }
            if (System.currentTimeMillis() - start > timeoutMillis) {
                return false;
            }
    
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    
        return false; 
    }
    
    public void startLogReplication() {
        if (raftNodeState.getRole() != Role.LEADER) return;

        for (String peerUrl : raftNodeState.getPeerUrls()) {
            if (!pendingReplication.getOrDefault(peerUrl, false)) {
                pendingReplication.put(peerUrl, true);
                replicationExecutor.submit(() -> replicateToFollowerLoop(peerUrl));
            }
        }
    }
        
    private void replicateToFollowerLoop(String peerUrl) {
        // the frequency of heartbeats depends on replicateToFollowerLoop
        int backoffMs = raftConfig.getHeartbeatIntervalMillis();
        while (raftNodeState.getRole() == Role.LEADER) {
            boolean success = replicateToFollower(peerUrl);
            if (success) {
                backoffMs = raftConfig.getHeartbeatIntervalMillis();
                updateCommitIndex(); 
            } else {
                backoffMs = Math.min(backoffMs * 2, raftConfig.getReplicationBackoffMaxMillis());
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
        if (raftNodeState.getRole() != Role.LEADER) return false;

        int ni = nextIndex.get(peerUrl);
        int prevLogIndex = ni - 1;
        int prevLogTerm = (prevLogIndex >= 0) ? raftLog.getTermAt(prevLogIndex) : 0;
        List<LogEntry> entries = raftLog.getEntriesFrom(ni);

        AppendEntryDTO dto = new AppendEntryDTO(
            raftNodeState.getCurrentTerm(),
            raftNodeState.getNodeId(),
            prevLogIndex,
            prevLogTerm,
            entries,
            raftLog.getCommitIndex()
        );

        try {
            AppendEntryResponseDTO response = sendAppendEntries(peerUrl, dto);
            if (response.getTerm() > raftNodeState.getCurrentTerm()) {
                raftStateManager.becomeFollower(response.getTerm());
                return false;
            }

            if (response.isSuccess()) {
                nextIndex.put(peerUrl, ni + entries.size());
                matchIndex.put(peerUrl, ni + entries.size() - 1);
                return true;
            } else {
                nextIndex.put(peerUrl, Math.max(0, ni - 1)); // backtrack
                return false;
            }
        } catch (Exception e) {
            return false; 
        }
    }

    private AppendEntryResponseDTO sendAppendEntries(String peerUrl, AppendEntryDTO dto) {
        try {
            String url = peerUrl + "/raft/appendEntries";
            ResponseEntity<AppendEntryResponseDTO> response =
                restTemplate.postForEntity(url, dto, AppendEntryResponseDTO.class);
            return response.getBody() != null ? response.getBody() : new AppendEntryResponseDTO(-1, false);
        } catch (Exception e) {
            return new AppendEntryResponseDTO(-1, false);
        }
    }

    private void updateCommitIndex() {
        int majority = (raftNodeState.getPeerUrls().size() + 1) / 2 + 1;
        int currentTerm = raftNodeState.getCurrentTerm();
    
        for (int i = raftLog.getLastIndex(); i > raftLog.getCommitIndex(); i--) {
            int count = 1;
            for (int index : matchIndex.values()) {
                if (index >= i) count++;
            }
            if (count >= majority && raftLog.getTermAt(i) == currentTerm) {
                raftLog.setCommitIndex(i);
                // async
                applyCommittedEntries();
                break;
            }
        }
    }

    // disk writes are costly so is offloaded via an a dedicated thread
    private void applyCommittedEntries() {
        applyExecutor.submit(() -> {
            int commitIndex = raftLog.getCommitIndex();
            int lastApplied = raftNodeState.getLastApplied();
            for (int i = lastApplied + 1; i <= commitIndex; i++) {
                try {
                    LogEntry entry = raftLog.getEntryAt(i);
                    stateMachine.apply(entry);
                    raftNodeState.setLastApplied(i);
                } catch (Exception e) {
                    System.out.println("State machine apply failed at index " + i + ": " + e.getMessage());
                    System.exit(1);
                }
            }
        });
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
            raftStateManager.becomeFollower(leaderTerm);
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

        raftStateManager.resetElectionTimer();
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
