import java.util.List;

public class RaftConfig {
    private final long heartbeatIntervalMillis;
    private final long electionTimeoutMillisMin;
    private final long electionTimeoutMillisMax;
    private final long clientRequestTimeoutMillis;
    private final long replicationBackoffMaxMillis;
    private final long electionRpcTimeoutMillis; // e.g., default = 300
    private final List<String> initialNodeUrls;
}
