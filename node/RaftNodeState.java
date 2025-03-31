@Getter
@Setter
@Component
@RequiredArgsConstructor
public class RaftNodeState {
    // ──────────────── Non-volatile state ────────────────
    private final int nodeId;            // fixed identity
    private int currentTerm = 0;         // must be persisted
    private Integer votedFor = null;     // must be persisted

    // ──────────────── Volatile state ─────────────────────
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;   // null = unknown
    private int lastApplied = 0;            // not persisted
}
