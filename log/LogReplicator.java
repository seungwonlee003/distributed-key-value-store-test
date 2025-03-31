@Component
@RequiredArgsConstructor
    public class LogReplicator {
    private final RaftConfig config;
    private final RaftLog log;
    private final RaftStateManager stateManager;
    private final RaftNodeState nodeState;
    private final RestTemplate restTemplate;
    private final StateMachine stateMachine;

    // Initialized directly in field declaration
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();
    private final Map<String, Boolean> pendingReplication = new ConcurrentHashMap<>();
    private final ExecutorService applyExecutor = Executors.newSingleThreadExecutor();

    // Cannot be final due to dynamic init
    private ScheduledExecutorService executor;

    @PostConstruct
    private void initExecutor() {
        this.executor = Executors.newScheduledThreadPool(config.getPeerUrlList().size());
    }

    public void initializeIndices() {
        int lastIndex = log.getLastIndex();
        for (String peer : config.getPeerUrlList()) {
            nextIndex.put(peer, lastIndex + 1);
            matchIndex.put(peer, 0);
        }
    }

    public void start() {
        if (nodeState.getRole() != Role.LEADER) return;
        for (String peer : config.getPeerUrlList()) {
            if (!pendingReplication.getOrDefault(peer, false)) {
                pendingReplication.put(peer, true);
                executor.submit(() -> replicateLoop(peer));
            }
        }
    }

    private void replicateLoop(String peer) {
        int backoff = config.getHeartbeatIntervalMillis();
        while (nodeState.getRole() == Role.LEADER) {
            boolean ok = replicate(peer);
            if (ok) {
                backoff = config.getHeartbeatIntervalMillis();
                updateCommitIndex();
            } else {
                backoff = Math.min(backoff * 2, config.getReplicationBackoffMaxMillis());
            }
            try { 
              Thread.sleep(backoff); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); 
                break;
            }
        }
        pendingReplication.put(peer, false);
    }

    private boolean replicate(String peer) {
        int ni = nextIndex.get(peer);
        int prevIdx = ni - 1;
        int prevTerm = (prevIdx >= 0) ? log.getTermAt(prevIdx) : 0;
        List<LogEntry> entries = log.getEntriesFrom(ni);

        AppendEntryDTO dto = new AppendEntryDTO(
            nodeState.getCurrentTerm(), nodeState.getNodeId(),
            prevIdx, prevTerm, entries, log.getCommitIndex()
        );

        try {
            String url = peer + "/raft/appendEntries";
            ResponseEntity<AppendEntryResponseDTO> res = restTemplate.postForEntity(url, dto, AppendEntryResponseDTO.class);
            AppendEntryResponseDTO body = res.getBody() != null ? res.getBody() : new AppendEntryResponseDTO(-1, false);
            if (body.getTerm() > nodeState.getCurrentTerm()) {
                stateManager.becomeFollower(body.getTerm());
                return false;
            }
            if (body.isSuccess()) {
                nextIndex.put(peer, ni + entries.size());
                matchIndex.put(peer, ni + entries.size() - 1);
                return true;
            } else {
                nextIndex.put(peer, Math.max(0, ni - 1));
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private void updateCommitIndex() {
        int majority = (config.getPeerUrlList().size() + 1) / 2 + 1;
        int term = nodeState.getCurrentTerm();
        for (int i = log.getLastIndex(); i > log.getCommitIndex(); i--) {
            int count = 1;
            for (int idx : matchIndex.values()) {
                if (idx >= i) count++;
            }
            if (count >= majority && log.getTermAt(i) == term) {
                log.setCommitIndex(i);
                applyExecutor.submit(() -> applyEntries());
                break;
            }
        }
    }

    private void applyEntries() {
        int commit = log.getCommitIndex();
        int lastApplied = nodeState.getLastApplied();
        for (int i = lastApplied + 1; i <= commit; i++) {
            try {
                LogEntry entry = log.getEntryAt(i);
                stateMachine.apply(entry);
                nodeState.setLastApplied(i);
            } catch (Exception e) {
                System.out.println("Failed to apply entry " + i);
                System.exit(1);
            }
        }
    }
}
