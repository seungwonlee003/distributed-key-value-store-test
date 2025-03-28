import java.util.List;

public class RaftConfig {
    
    // Heartbeat interval in milliseconds: how often the leader sends heartbeat AppendEntries RPCs.
    private final long heartbeatIntervalMillis;
    
    // Election timeout: lower bound in milliseconds.
    // If a follower hasn't received a heartbeat within a random time between these bounds, it starts an election.
    private final long electionTimeoutMillisMin;
    
    // Election timeout: upper bound in milliseconds.
    private final long electionTimeoutMillisMax;
    
    // List of node URLs for the initial cluster (for example, 5 nodes).
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
