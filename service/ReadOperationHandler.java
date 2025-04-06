@Service
@RequiredArgsConstructor
public class ReadOperationHandler {
    private final RaftNodeState raftNodeState;
    private final RaftLogManager raftLogManager;
    private final KVStore kvStore;
    private final LeadershipManager leadershipManager;

    public String handleRead(String key) throws IllegalStateException {
        leadershipManager.confirmLeadership();
        return kvStore.get(key);
    }
}
