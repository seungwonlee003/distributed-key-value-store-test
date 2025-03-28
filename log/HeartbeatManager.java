import java.util.concurrent.*;

public class HeartbeatManager {
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatFuture;

    public HeartbeatManager(RaftNode raftNode, ElectionManager electionManager) {
        this.raftNode = raftNode;
        this.electionManager = electionManager;
    }
    
    public void startHeartbeats(){
        stopHeartbeats();
        electionManager.cancelElectionTimerIfRunning();
        heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (raftNode.getRole() == Role.LEADER) {
                raftNode.startLogReplication(); // Empty entries act as heartbeat
            }
        }, 0, 1000, TimeUnit.MILLISECONDS); // 150ms heartbeat interval
    }

    public void stopHeartbeats() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }
}
