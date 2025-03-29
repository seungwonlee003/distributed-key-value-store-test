public class AppendEntriesHandler {
    private final RaftLog log;
    private final RaftStateManager stateManager;
    private final RaftNodeState nodeState;
    private final StateMachine stateMachine;
    private final ExecutorService applyExecutor = Executors.newSingleThreadExecutor();

    public AppendEntriesHandler(RaftLog log, RaftStateManager sm, RaftNodeState ns, StateMachine smachine) {
        this.log = log;
        this.stateManager = sm;
        this.nodeState = ns;
        this.stateMachine = smachine;
    }

    public synchronized AppendEntryResponseDTO handle(AppendEntryDTO dto) {
        int term = nodeState.getCurrentTerm();
        int leaderTerm = dto.getTerm();

        if (leaderTerm < term) return new AppendEntryResponseDTO(term, false);
        if (leaderTerm > term) {
            stateManager.becomeFollower(leaderTerm);
            term = leaderTerm;
        }

        if (dto.getPrevLogIndex() > 0 &&
            (!log.containsEntryAt(dto.getPrevLogIndex()) ||
             log.getTermAt(dto.getPrevLogIndex()) != dto.getPrevLogTerm())) {
            return new AppendEntryResponseDTO(term, false);
        }

        int index = dto.getPrevLogIndex() + 1;
        List<LogEntry> entries = dto.getEntries();
        if (!entries.isEmpty()) {
            if (log.containsEntryAt(index) && log.getTermAt(index) != entries.get(0).getTerm()) {
                log.deleteFrom(index);
            }
            log.appendAll(entries);
        }

        if (dto.getLeaderCommit() > log.getCommitIndex()) {
            int lastNew = dto.getPrevLogIndex() + entries.size();
            log.setCommitIndex(Math.min(dto.getLeaderCommit(), lastNew));
            applyExecutor.submit(() -> applyEntries());
        }

        stateManager.resetElectionTimer();
        return new AppendEntryResponseDTO(term, true);
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
