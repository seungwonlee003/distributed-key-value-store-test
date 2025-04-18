import jakarta.annotation.PreDestroy;
import java.util.Random;
import java.util.concurrent.*;

import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class ElectionTimer {
    private final RaftConfig raftConfig;
    private final RaftNode raftNode;
    private final ElectionManager electionManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = new Random();
    private ScheduledFuture<?> electionFuture;

    public synchronized void reset() {
        if (electionFuture != null) {
            electionFuture.cancel(false); // Cancel without interrupting running tasks
        }

        long minTimeout = raftConfig.getElectionTimeoutMillisMin();
        long maxTimeout = raftConfig.getElectionTimeoutMillisMax();
        long timeout = minTimeout + random.nextInt((int)(maxTimeout - minTimeout + 1)); // Inclusive max

        electionFuture = scheduler.schedule(() -> electionManager.startElection(), timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancel() {
        if (electionFuture != null) {
            electionFuture.cancel(false);
        }
    }

    @PreDestroy
    public synchronized void shutdown() {
        scheduler.shutdownNow();
    }
}
