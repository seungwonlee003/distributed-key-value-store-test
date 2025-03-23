import java.util.concurrent.*;

public class HeartbeatManager {
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> heartbeatFuture;

    public HeartbeatManager(RaftNode raftNode, ElectionManager electionManager) {
        this.raftNode = raftNode;
        this.electionManager = electionManager;
    }

    public void startHeartbeats() {
        stopHeartbeats();
        electionManager.cancelElectionTimerIfRunning();
        RaftNodeState state = raftNode.getState();
        if (state.getRole() != Role.LEADER){
            electionManager.resetElectionTimer();
            stopHeartbeats();
            return;
        }

        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            synchronized (this) {
                if (state.getRole() == Role.LEADER) {
                    try {
                        raftNode.getRaftLogManager().replicateLogToFollowers(null);
                    } catch (Exception e) {
                        System.err.println("Heartbeat failed: " + e.getMessage());
                    }
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    public void stopHeartbeats() {
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }
}
