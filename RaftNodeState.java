import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class RaftNodeState {
    // Persistent state on all servers:
    private int currentTerm = 0;  
    private Integer votedFor = null; 
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    // Volatile state on all servers:
    private int commitIndex = 0;  // Index of highest log entry known to be committed
    private int lastApplied = 0;  // Index of highest log entry applied to state machine

    // Server identity and leadership info:
    private final int nodeId;       // Unique identifier for this node
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;  

    // Volatile state on candidates:
    private final Set<Integer> votesReceived = new HashSet<>();

    // Volatile state on leaders (reinitialized after election):
    // Keyed by peer node id, tracking the next log index to send and highest replicated index
    private final Map<Integer, Integer> nextIndex = new HashMap<>();
    private final Map<Integer, Integer> matchIndex = new HashMap<>();

    // Constructor: requires a unique nodeId
    public RaftNodeState(int nodeId) {
        this.nodeId = nodeId;
    }

    // ---------------- Getters and Setters ---------------- //

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public Integer getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(Integer votedFor) {
        this.votedFor = votedFor;
    }

    public List<LogEntry> getLog() {
        return log;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    public int getNodeId() {
        return nodeId;
    }

    public Role getCurrentRole() {
        return currentRole;
    }

    public void setCurrentRole(Role currentRole) {
        this.currentRole = currentRole;
    }

    public Integer getCurrentLeader() {
        return currentLeader;
    }

    public void setCurrentLeader(Integer currentLeader) {
        this.currentLeader = currentLeader;
    }

    public Set<Integer> getVotesReceived() {
        return votesReceived;
    }

    public Map<Integer, Integer> getNextIndex() {
        return nextIndex;
    }

    public Map<Integer, Integer> getMatchIndex() {
        return matchIndex;
    }

    // ---------------- Helper Methods ---------------- //

    /**
     * Returns the index of the last log entry.
     * Depending on your log indexing (starting at 1 or 0), adjust accordingly.
     */
    public int getLastLogIndex() {
        // Assuming the first log entry is at index 1.
        return log.size();
    }

    /**
     * Returns the term of the last log entry, or 0 if the log is empty.
     */
    public int getLastLogTerm() {
        if (log.isEmpty()) {
            return 0;
        }
        return log.get(log.size() - 1).getTerm();
    }
}
