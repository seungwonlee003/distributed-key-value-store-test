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

    /**
     * Handle incoming AppendEntries requests either from /append from /write or heartbeats(Follower side).
     */
    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        int currentTerm = raftNodeState.getCurrentTerm();
        int leaderTerm = dto.getTerm();
        int prevLogIndex = dto.getPrevLogIndex();
        int prevLogTerm = dto.getPrevLogTerm();
        List<LogEntry> entries = dto.getEntries();
        int leaderCommit = dto.getLeaderCommit();

        // If the leader's term is behind, reject
        if (leaderTerm < currentTerm) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        // If leaderTerm is newer, convert to follower
        if (leaderTerm > currentTerm) {
            raftNodeState.setCurrentTerm(leaderTerm);
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            raftNode.resetElectionTimer();
            currentTerm = leaderTerm;
        }

        // Check if log contains an entry at prevLogIndex with matching term
        if (prevLogIndex > 0 &&
            (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            return new AppendEntryResponseDTO(currentTerm, false);
        }

        // Append new entries, delete conflicts
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

        // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
        }

        raftNode.resetElectionTimer();
        return new AppendEntryResponseDTO(currentTerm, true);
    }

    /**
     * Blocking method that replicates the new entries to followers and 
     * either throws an exception if a majority cannot be reached,
     * or returns normally once commitIndex is updated.
     */
    public synchronized replicateLogToFollowers(List<LogEntry> newEntries, long overallTimeoutMs) throws Exception {
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
        int majority = (raftNode.getPeerUrls().size() + 2) / 2;
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
            .orTimeout(overallTimeoutMs, TimeUnit.MILLISECONDS) // Per-task timeout
            .exceptionally(throwable -> {
                // Timeout or error treated as failure; do nothing
                return null;
            }))
            .collect(Collectors.toList());
    
        // Wait for majority or timeout
        boolean majorityAchieved = majorityLatch.await(overallTimeoutMs, TimeUnit.MILLISECONDS);
    
        if (!majorityAchieved || successes.get() < majority) {
            throw new RuntimeException("Failed to achieve majority commit");
        }
    
        // Commit logic (same as original)
        int newCommitIndex = raftLog.getCommitIndex();
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
        }
    }

    /**
     * A blocking helper that replicates entries [ni..targetIndex] to a single follower
     * with a synchronous RestTemplate call. Returns true on success.
     *
     * In a full Raft system, you'd keep retrying in a heartbeat loop until the follower catches up.
     * Here we do a single attempt (plus we decrement nextIndex if it fails).
     */
    private boolean replicateToFollower(
            String peerUrl, int currentTerm, int targetIndex) {

        synchronized (this) {
            if (raftNodeState.getRole() != Role.LEADER) {
                return false;
            }

            int ni = nextIndex.get(peerUrl);
            int prevLogIndex = Math.max(0, ni - 1);
            int prevLogTerm = (prevLogIndex > 0) ? raftLog.getTermAt(prevLogIndex) : 0;
            List<LogEntry> entriesToSend = getEntriesFrom(ni, targetIndex);

            AppendEntryDTO dto = new AppendEntryDTO(
                currentTerm,
                raftNodeState.getNodeId(),
                prevLogIndex,
                prevLogTerm,
                entriesToSend,
                raftLog.getCommitIndex()
            );

            // Synchronous call with built-in timeouts in the RestTemplate
            AppendEntryResponseDTO response = sendAppendEntries(peerUrl, dto);

            if (response.getTerm() > currentTerm) {
                // Found a higher term, step down
                raftNodeState.setCurrentTerm(response.getTerm());
                raftNodeState.setRole(Role.FOLLOWER);
                raftNodeState.setVotedFor(null);
                raftNode.resetElectionTimer();
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
    }

    /**
     * Fully synchronous call to the follower's /appendEntries endpoint.
     * We rely on the RestTemplate's internal timeouts (connect/read) 
     * and the 'singleCallTimeoutMs' can be further enforced in the template if needed.
     */
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

    /**
     * Return entries from [startIndex..endIndex].
     */
    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        synchronized (raftLog) {
            List<LogEntry> entries = new ArrayList<>();
            for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
                entries.add(raftLog.getEntryAt(i));
            }
            return entries;
        }
    }

    public RaftNodeState getRaftNodeState() {
        return this.raftNodeState;
    }
}
