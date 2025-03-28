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

    // for starting heartbeats only
    public void startHeartbeats(){
        stopHeartbeats();
        electionManager.cancelElectionTimerIfRunning();
        heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (raftNode.getRole() == Role.LEADER) {
                raftNode.startLogReplication();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void stopHeartbeats() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }
}
