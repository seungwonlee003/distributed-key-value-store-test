@Getter
@Setter
@Component
@RequiredArgsConstructor
public class RaftNodeState {
    private RaftConfig config;
    // ──────────────── Non-volatile state ────────────────
    private final int nodeId;            // fixed identity
    private int currentTerm = 0;         // must be persisted
    private Integer votedFor = null;     // must be persisted

    // ──────────────── Volatile state ─────────────────────
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;   // null = unknown
    private int lastApplied = 0;            // not persisted

    public void setCurrentLeader(Integer leaderId) {
        if (!Objects.equals(this.currentLeader, leaderId)) {
            System.out.println("New leader detected: Node " + leaderId);
        }
        this.currentLeader = leaderId;
    }

    public String getCurrentLeaderUrl(){
        return config.getPeerUrls().get(currentLeader);
    }
}
