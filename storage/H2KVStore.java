package com.example.raft.storage;

import org.springframework.stereotype.Component;
import java.sql.*;

@Component
public class H2KVStore implements KVStore {
    private final Connection connection;

    public H2KVStore() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:h2:./data/kvstore", "sa", "");
        createTables();
    }

    private void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS kv_store (key VARCHAR(255) PRIMARY KEY, value VARCHAR(255))");
            stmt.execute("CREATE TABLE IF NOT EXISTS client_store (client_id VARCHAR(255) PRIMARY KEY, last_request_id BIGINT)");
        }
    }

    @Override
    public void put(String key, String value) {
        try (PreparedStatement pstmt = connection.prepareStatement(
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
        try (PreparedStatement pstmt = connection.prepareStatement(
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
        try (PreparedStatement pstmt = connection.prepareStatement(
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
        try (PreparedStatement pstmt = connection.prepareStatement(
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
    public Long getLastRequestId(String clientId) {
        try (PreparedStatement pstmt = connection.prepareStatement(
                "SELECT last_request_id FROM client_store WHERE client_id = ?")) {
            pstmt.setString(1, clientId);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next() ? rs.getLong("last_request_id") : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get last request ID", e);
        }
    }

    @Override
    public void setLastRequestId(String clientId, Long requestId) {
        try (PreparedStatement pstmt = connection.prepareStatement(
                "MERGE INTO client_store (client_id, last_request_id) VALUES (?, ?)")) {
            pstmt.setString(1, clientId);
            pstmt.setLong(2, requestId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set last request ID", e);
        }
    }
}
