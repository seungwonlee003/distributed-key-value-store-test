import java.util.Random;
import java.util.concurrent.*;

public class ElectionTimer {
    private final RaftConfig raftConfig;
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();
    private ScheduledFuture<?> electionFuture;

    public ElectionTimer(RaftNode raftNode, ElectionManager electionManager) {
        this.raftNode = raftNode;
        this.electionManager = electionManager;
        this.heartbeatManager = heartbeatManager;
    }

    public synchronized void reset() {
        cancel();
    
        long minTimeout = raftConfig.getElectionTimeoutMillisMin();
        long maxTimeout = raftConfig.getElectionTimeoutMillisMax();
        long timeout = minTimeout + random.nextInt((int)(maxTimeout - minTimeout));
    
        electionFuture = scheduler.schedule(() -> {
            electionManager.startElection();
        }, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(false);
            electionFuture = null;
        }
    }
}
