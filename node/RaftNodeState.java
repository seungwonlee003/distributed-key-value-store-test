@Getter
@Setter
@Component
@RequiredArgsConstructor
public class RaftNodeState {
    // non-volatile
    private final int nodeId;
    private int currentTerm = 0;
    private Integer votedFor = null;

    // volatile
    private Role currentRole = Role.FOLLOWER;
    private int currentLeader;
    private int lastApplied = 0;
}
