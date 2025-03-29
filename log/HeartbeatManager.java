import java.util.concurrent.*;

public class HeartbeatManager {
    private final RaftLogManager raftLogManager;
    private final RaftNodeState raftNodeState;
    private final ElectionManager electionManager;
    ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatFuture;

    // for starting heartbeats only
    public void startHeartbeats(){
        stopHeartbeats();
        electionManager.cancelElectionTimerIfRunning();
        heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (raftNodeState.getRole() == Role.LEADER) {
                raftLogManager.startLogReplication();
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
