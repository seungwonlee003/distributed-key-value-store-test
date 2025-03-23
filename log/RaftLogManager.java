@Service
public class RaftLogManager {

    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftNodeState raftNodeState;
    private final Map<String, Integer> nextIndex;
    private final Map<String, Integer> matchIndex;
    private final StateMachine stateMachine;
    private final HeartManager heartManager;

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.raftNodeState = raftNode.getState();
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.stateMachine = new StateMachine();
        this.heartManager = new HeartManager();
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

        // If leaderTerm is newer, convert to follower
        if (leaderTerm > currentTerm) {
            raftNodeState.setCurrentTerm(leaderTerm);
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            heartbeatManager.stopHeartBeats();
            heartbeatManager.resetElectionTimer();
            currentTerm = leaderTerm;
        }

        // Check if log contains an entry at prevLogIndex with matching term
        if (prevLogIndex > 0 &&
            (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        // Append new entries, delete conflicts
        appendEntries(prevLogIndex, entries);

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
            applyCommittedEntries(); 
        }

        heartbeatManager.resetElectionTimer();
        return new AppendEntryResponseDTO(currentTerm, true);
    }

    private void appendEntries(int prevLogIndex, List<LogEntry> entries) {
        int index = prevLogIndex + 1;
        for (LogEntry entry : entries) {
            if (raftLog.containsEntryAt(index) && raftLog.getTermAt(index) != entry.getTerm()) {
                // Delete any existing conflicting entry and everything after
                raftLog.deleteFrom(index);
            }
            if (!raftLog.containsEntryAt(index)) {
                raftLog.append(entry);
            }
            index++;
        }
    }
    
    public synchronized void replicateLogToFollowers(List<LogEntry> newEntries) throws Exception {
        if (raftNodeState.getRole() != Role.LEADER) {
            throw new IllegalStateException("Not leader");
        }
    
        if (newEntries != null && !newEntries.isEmpty()) {
            newEntries.forEach(raftLog::append);
        }
    
        int finalIndexOfNewEntries = raftLog.getLastIndex();
        int currentTerm = raftNodeState.getCurrentTerm();
        if (nextIndex.isEmpty()) {
            initializeIndices();
        }
    
        ExecutorService executor = raftNode.getAsyncExecutor();
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;

        AtomicInteger successes = new AtomicInteger(1); // Leader counts itself
        CountDownLatch majorityLatch = new CountDownLatch(majority - 1); // Wait for majority minus leader
    
        // Launch replication tasks asynchronously
        List<CompletableFuture<Void>> futures = raftNode.getPeerUrls().stream()
            .map(peerUrl -> CompletableFuture.runAsync(() -> {
                boolean success = replicateToFollower(peerUrl, currentTerm, finalIndexOfNewEntries);
                if (success) {
                    if (successes.incrementAndGet() <= majority) {
                        majorityLatch.countDown();
                    }
                }
            }, executor)
            .exceptionally(throwable -> {
                // Timeout or error treated as failure; do nothing
                return null;
            }))
            .collect(Collectors.toList());
    
        // Wait for majority or timeout
        boolean majorityAchieved = majorityLatch.await(1000, TimeUnit.MILLISECONDS);
    
        if (!majorityAchieved || successes.get() < majority) {
            throw new RuntimeException("Failed to achieve majority commit");
        }
    
        commitLogEntries(finalIndexOfNewEntries);
    }

    private void commitLogEntries(int finalIndexOfNewEntries) {
        int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
        int newCommitIndex = raftLog.getCommitIndex();
        int currentTerm = raftNodeState.getCurrentTerm();

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

    private boolean replicateToFollower(
            String peerUrl, int currentTerm, int targetIndex) {

        if (raftNodeState.getRole() != Role.LEADER) {
            return false;
        }

        int ni = nextIndex.get(peerUrl);
        int prevLogIndex = Math.max(0, ni - 1);
        int prevLogTerm = (prevLogIndex > 0) ? raftLog.getTermAt(prevLogIndex) : 0;
        List<LogEntry> entriesToSend = raftLog.getEntriesFrom(ni, targetIndex);

        AppendEntryDTO dto = new AppendEntryDTO(
            currentTerm,
            raftNodeState.getNodeId(),
            prevLogIndex,
            prevLogTerm,
            entriesToSend,
            raftLog.getCommitIndex()
        );

        AppendEntryResponseDTO response = sendAppendEntries(peerUrl, dto);

        if (response.getTerm() > currentTerm) {
            // Found a higher term, step down
            raftNodeState.setCurrentTerm(response.getTerm());
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            heartbeatManager.resetElectionTimer();
            return false;
        }

        if (response.isSuccess()) {
            // Update nextIndex/matchIndex
            nextIndex.put(peerUrl, ni + entriesToSend.size());
            matchIndex.put(peerUrl, ni + entriesToSend.size() - 1);
            return true;
        } else {
            // Conflict => decrement nextIndex
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

            if (response.getBody() != null) {
                return response.getBody();
            } else {
                return new AppendEntryResponseDTO(-1, false);
            }
        } catch (Exception e) {
            return new AppendEntryResponseDTO(-1, false);
        }
    }

    public RaftNodeState getRaftNodeState() {
        return this.raftNodeState;
    }
    
    private void applyCommittedEntries() {
        int commitIndex = raftLog.getCommitIndex();
        int lastApplied = raftNodeState.getLastApplied();
        for (int i = lastApplied + 1; i <= commitIndex; i++) {
            LogEntry entry = raftLog.getEntryAt(i);
            raftNode.getStateMachine().apply(entry);
            raftNodeState.setLastApplied(i);
        }
    }
}
