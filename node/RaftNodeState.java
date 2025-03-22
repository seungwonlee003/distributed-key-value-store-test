import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class RaftNodeState {
    // Persistent state on all servers:
    private int currentTerm = 0;  
    private Integer votedFor = null; 
    
    // Server identity and leadership info:
    private final int nodeId;       // Unique identifier for this node
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;  

    private int lastApplied = 0; // Add this field

    // Existing fields and methods...

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    // Volatile state on candidates:
    private final Set<Integer> votesReceived = new HashSet<>();

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
}
