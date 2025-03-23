import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class RaftNodeState {
    // Persistent state on all servers:
    private int currentTerm = 0;  
    private Integer votedFor = null; 
    
    private final int nodeId;       
    // start with a role of follower
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;  

    private int lastApplied = 0; // Add this field

    public int getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
    }

    // get nodeId from the application.properties
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
}
