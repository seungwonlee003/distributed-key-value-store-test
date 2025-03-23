import java.util.Random;
import java.util.concurrent.*;

public class ElectionTimer {
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();
    private final int electionTimeoutMin = 3000; // in milliseconds
    private final int electionTimeoutMax = 5000; // in milliseconds
    private ScheduledFuture<?> electionFuture;

    public ElectionTimer(RaftNode raftNode, ElectionManager electionManager) {
        this.raftNode = raftNode;
        this.electionManager = electionManager;
        this.heartbeatManager = heartbeatManager;
    }

    public synchronized void reset() {
        cancel();
        int timeout = electionTimeoutMin + random.nextInt(electionTimeoutMax - electionTimeoutMin);
        electionFuture = scheduler.schedule(() -> {
            electionManager.startElection();
        }, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (electionFuture != null && !electionFuture.isDone()) {
            electionFuture.cancel(false);
        }
    }
}
