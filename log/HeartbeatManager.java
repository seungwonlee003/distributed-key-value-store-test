import java.util.concurrent.*;

public class HeartbeatManager {
    private final RaftNode raftNode;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> heartbeatFuture;

    public HeartbeatManager(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public void startHeartbeats() {
        stopHeartbeats();
        RaftNodeState state = raftNode.getState();
        if (state.getRole() != Role.LEADER){
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
