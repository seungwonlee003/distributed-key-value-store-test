@Getter
@Setter
@Component
@RequiredArgsConstructor
public class RaftNodeState {
    private final int nodeId;

    private int currentTerm = 0;
    private Integer votedFor = null;
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;
    private int lastApplied = 0;
    private List<String> peerUrls; // Assuming URLs
}
