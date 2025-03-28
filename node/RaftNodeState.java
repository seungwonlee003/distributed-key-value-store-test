import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

// Persistent state on all servers:
@Getter
@Setter
public class RaftNodeState {
    private int currentTerm = 0;  
    private Integer votedFor = null; 
    
    private final int nodeId;       
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;  

    private int lastApplied = 0; 

    // get nodeId from the application.properties
    public RaftNodeState(int nodeId) {
        this.nodeId = nodeId;
    }
}
