@Component
@RequiredArgsConstructor
public class H2KVStore implements KVStore {

    private final DataSource dataSource;

    @PostConstruct
    public void init() throws SQLException {
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS kv_store (key VARCHAR(255) PRIMARY KEY, value VARCHAR(255))");
            stmt.execute("CREATE TABLE IF NOT EXISTS client_store (client_id VARCHAR(255) PRIMARY KEY, last_sequence_number BIGINT)");
        }
    }

    @Override
    public void put(String key, String value) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "MERGE INTO kv_store (key, value) VALUES (?, ?)")) {
            pstmt.setString(1, key);
            pstmt.setString(2, value);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to put key-value pair", e);
        }
    }

    @Override
    public void remove(String key) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "DELETE FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            int rows = pstmt.executeUpdate();
            if (rows == 0) {
                throw new IllegalStateException("Key '" + key + "' does not exist for removal");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to remove key", e);
        }
    }

    @Override
    public String get(String key) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT value FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getString("value") : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get value for key", e);
        }
    }

    @Override
    public boolean containsKey(String key) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM kv_store WHERE key = ?")) {
            pstmt.setString(1, key);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check if key exists", e);
        }
    }

    @Override
    public Long getLastSequenceNumber(String clientId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "SELECT last_sequence_number FROM client_store WHERE client_id = ?")) {
            pstmt.setString(1, clientId);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getLong("last_sequence_number") : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get last sequence number", e);
        }
    }

    @Override
    public void setLastSequenceNumber(String clientId, Long sequenceNumber) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(
                     "MERGE INTO client_store (client_id, last_sequence_number) VALUES (?, ?)")) {
            pstmt.setString(1, clientId);
            pstmt.setLong(2, sequenceNumber);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set last sequence number", e);
        }
    }
}
