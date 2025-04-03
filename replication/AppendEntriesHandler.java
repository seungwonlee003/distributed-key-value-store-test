@Component
@RequiredArgsConstructor
public class AppendEntriesHandler {
    private final RaftLog log;
    private final RaftStateManager stateManager;
    private final RaftNodeState nodeState;
    private final StateMachine stateMachine;
    private ExecutorService applyExecutor = Executors.newSingleThreadExecutor();

    public synchronized AppendEntryResponseDTO handle(AppendEntryDTO dto) {
        int term = nodeState.getCurrentTerm();
        int leaderTerm = dto.getTerm();

        if (leaderTerm < term) return new AppendEntryResponseDTO(term, false);
        if (leaderTerm > term) {
            stateManager.becomeFollower(leaderTerm);
            term = leaderTerm;
        }

        // set currentLeader
        nodeState.setCurrentLeader(dto.getLeaderId());

        if (dto.getPrevLogIndex() > 0 &&
            (!log.containsEntryAt(dto.getPrevLogIndex()) ||
             log.getTermAt(dto.getPrevLogIndex()) != dto.getPrevLogTerm())) {
            return new AppendEntryResponseDTO(term, false);
        }
        
        int index = dto.getPrevLogIndex() + 1;
        List<LogEntry> entries = dto.getEntries();
        if (!entries.isEmpty()) {
            for (int i = 0; i < entries.size(); i++) {
                int logIndex = index + i;
                if (log.containsEntryAt(logIndex) && log.getTermAt(logIndex) != entries.get(i).getTerm()) {
                    log.deleteFrom(logIndex);
                    log.appendAll(entries.subList(i, entries.size()));
                    break;
                }
            }
            if (!log.containsEntryAt(index)) { // No conflicts found, append all
                log.appendAll(entries);
            }
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
