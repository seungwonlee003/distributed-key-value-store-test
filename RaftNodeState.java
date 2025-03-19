public class RaftNodeState {
    private int currentTerm = 0;  
    private Integer votedFor = null; 
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    private int commitIndex = 0;  // Index of highest log entry known to be committed
    private int lastApplied = 0;  // Index of highest log entry applied to state machine

    private final int nodeId;  // Unique identifier for this node
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;  

    // Election tracking
    private final Set<Integer> votesReceived = new HashSet<>();  

    // Volatile state on leaders (Reinitialized after election)
    private final Map<Integer, Integer> nextIndex = new HashMap<>();  // Next log entry index to send
    private final Map<Integer, Integer> matchIndex = new HashMap<>();  // Highest replicated index

    // Constructor
    public RaftNodeState(int nodeId) {
    }
