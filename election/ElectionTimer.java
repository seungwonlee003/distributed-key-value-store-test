import jakarta.annotation.PreDestroy;
import java.util.Random;
import java.util.concurrent.*;

@Component
@RequiredArgsConstructor
public class ElectionTimer {
    private final RaftConfig raftConfig;
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();

    public synchronized void reset() {
        cancel();

        long minTimeout = raftConfig.getElectionTimeoutMillisMin();
        long maxTimeout = raftConfig.getElectionTimeoutMillisMax();
        long timeout = minTimeout + random.nextInt((int)(maxTimeout - minTimeout));

        ScheduledExecutorService newScheduler = Executors.newSingleThreadScheduledExecutor();
        newScheduler.schedule(() -> electionManager.startElection(), timeout, TimeUnit.MILLISECONDS);
        this.scheduler = newScheduler;
    }

    public synchronized void cancel() {
        scheduler.shutdownNow();
        scheduler.getQueue().clear();
    }

    @PreDestroy
    public synchronized void shutdown() {
        scheduler.shutdownNow();
    }
}
