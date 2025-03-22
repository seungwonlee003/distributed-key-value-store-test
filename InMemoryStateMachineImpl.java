import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An in-memory implementation of the StateMachine interface for Raft.
 * This class maintains a key-value store and applies log entries to update its state.
 * It is thread-safe and supports basic operations like put, delete, and querying state.
 */
public class InMemoryStateMachine implements StateMachine {
    // Use ConcurrentHashMap for thread-safety, as Raft nodes may process log entries concurrently
    private final Map<String, String> state;

    /**
     * Default constructor initializing an empty in-memory state.
     */
    public InMemoryStateMachine() {
        this.state = new ConcurrentHashMap<>();
    }

    /**
     * Applies a log entry to the state machine.
     * Supported commands:
     * - "put key value": Stores the key-value pair.
     * - "delete key": Removes the key-value pair if it exists.
     * 
     * @param entry The log entry containing the command to apply.
     * @throws IllegalArgumentException if the command is malformed or unsupported.
     */
    @Override
    public void apply(LogEntry entry) {
        if (entry == null || entry.getCommand() == null) {
            throw new IllegalArgumentException("Log entry or command cannot be null");
        }

        String command = entry.getCommand().trim();
        if (command.isEmpty()) {
            throw new IllegalArgumentException("Command cannot be empty");
        }

        String[] parts = command.split("\\s+"); // Split on whitespace
        switch (parts[0].toLowerCase()) {
            case "put":
                if (parts.length != 3) {
                    throw new IllegalArgumentException("Put command requires format: 'put key value'");
                }
                state.put(parts[1], parts[2]);
                break;

            case "delete":
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Delete command requires format: 'delete key'");
                }
                state.remove(parts[1]);
                break;

            default:
                throw new IllegalArgumentException("Unsupported command: " + parts[0]);
        }
    }

    /**
     * Retrieves the value associated with a key.
     * This is a helper method for querying the state, not part of the Raft apply process.
     * 
     * @param key The key to look up.
     * @return The value associated with the key, or null if the key doesn't exist.
     */
    public String get(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return state.get(key);
    }

    /**
     * Checks if a key exists in the state.
     * 
     * @param key The key to check.
     * @return true if the key exists, false otherwise.
     */
    public boolean containsKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        return state.containsKey(key);
    }

    /**
     * Returns the current size of the state (number of key-value pairs).
     * 
     * @return The number of entries in the state.
     */
    public int size() {
        return state.size();
    }

    /**
     * Clears all entries in the state.
     * This is a utility method and should be used with caution, as it doesn't interact with Raft logs.
     */
    public void clear() {
        state.clear();
    }

    /**
     * Provides a snapshot of the current state as a Map.
     * Note: This returns a shallow copy to avoid exposing the internal state directly.
     * 
     * @return A copy of the current state.
     */
    public Map<String, String> getStateSnapshot() {
        return new ConcurrentHashMap<>(state);
    }

    /**
     * Utility method to apply a batch of log entries.
     * This can be used for initialization or recovery scenarios, though Raft typically applies entries one-by-one.
     * 
     * @param entries The list of log entries to apply.
     */
    public void applyBatch(List<LogEntry> entries) {
        if (entries == null) {
            throw new IllegalArgumentException("Entries list cannot be null");
        }
        for (LogEntry entry : entries) {
            apply(entry);
        }
    }
}
