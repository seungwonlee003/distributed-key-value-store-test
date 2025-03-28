import java.util.List;

public class RaftConfig {
    
    public static final long DEFAULT_HEARTBEAT_INTERVAL;
    public static final long DEFAULT_ELECTION_TIMEOUT_MIN = 1500;
    public static final long DEFAULT_ELECTION_TIMEOUT_MAX = 1500;
    private final List<String> initialNodeUrls;
    
    // Optional: specify a dedicated thread pool size for async tasks (heartbeats, elections, etc.)
    private final int asyncThreadPoolSize;
    
    // Optional: persist file location for state (currentTerm, votedFor, etc.)
    private final String persistenceFilePath;
    
    private RaftConfig(Builder builder) {
        this.heartbeatIntervalMillis = builder.heartbeatIntervalMillis;
        this.electionTimeoutMillisMin = builder.electionTimeoutMillisMin;
        this.electionTimeoutMillisMax = builder.electionTimeoutMillisMax;
        this.initialNodeUrls = builder.initialNodeUrls;
        this.asyncThreadPoolSize = builder.asyncThreadPoolSize;
        this.persistenceFilePath = builder.persistenceFilePath;
    }
    
    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public long getElectionTimeoutMillisMin() {
        return electionTimeoutMillisMin;
    }

    public long getElectionTimeoutMillisMax() {
        return electionTimeoutMillisMax;
    }

    public List<String> getInitialNodeUrls() {
        return initialNodeUrls;
    }

    public int getAsyncThreadPoolSize() {
        return asyncThreadPoolSize;
    }
    
    public String getPersistenceFilePath() {
        return persistenceFilePath;
    }
}
