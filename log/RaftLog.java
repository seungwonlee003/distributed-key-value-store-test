import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * A class to manage the log of entries in a Raft consensus system.
 * It handles appending, deleting, and querying log entries, as well as tracking the commit index
 * and notifying listeners when the commit index changes.
 */
public class RaftLog {
    // List to hold log entries, starting from index 1 as per Raft convention
    private final List<LogEntry> logEntries = new ArrayList<>();
    // Commit index: the highest log entry known to be committed
    private int commitIndex = 0;
    // Listeners for commit index changes, using a thread-safe collection
    private final List<Consumer<Integer>> commitIndexListeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor initializes the log with a dummy entry at index 0.
     * This simplifies indexing as Raft uses 1-based log indices.
     */
    public RaftLog() {
        logEntries.add(new LogEntry(0, null)); // Term 0, no command
    }

    /**
     * Appends a new log entry to the end of the log.
     * @param entry The log entry to append.
     */
    public void append(LogEntry entry) {
        logEntries.add(entry);
    }

    /**
     * Checks if the log contains an entry at the specified index.
     * @param index The index to check.
     * @return True if an entry exists at the index, false otherwise.
     */
    public boolean containsEntryAt(int index) {
        return index >= 1 && index < logEntries.size();
    }

    /**
     * Retrieves the term of the log entry at the specified index.
     * @param index The index of the log entry.
     * @return The term of the log entry, or -1 if the index is out of bounds.
     */
    public int getTermAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index).getTerm();
        }
        return -1; // Invalid term
    }

    /**
     * Deletes all log entries starting from the specified index to the end.
     * @param fromIndex The starting index from which to delete entries.
     */
    public void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            logEntries.subList(fromIndex, logEntries.size()).clear();
        }
    }

    /**
     * Gets the index of the last log entry.
     * @return The index of the last log entry, or 0 if only the dummy entry exists.
     */
    public int getLastIndex() {
        return logEntries.size() - 1;
    }

    /**
     * Gets the term of the last log entry.
     * @return The term of the last log entry, or 0 if only the dummy entry exists.
     */
    public int getLastTerm() {
        if (logEntries.size() > 1) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        return 0;
    }

    /**
     * Gets the current commit index.
     * @return The commit index.
     */
    public int getCommitIndex() {
        return commitIndex;
    }

    /**
     * Sets the commit index to a new value, ensuring it doesn't exceed the last log index.
     * Notifies all registered listeners if the commit index is updated.
     * @param newCommitIndex The new commit index.
     */
    public void setCommitIndex(int newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastIndex()) {
            commitIndex = newCommitIndex;
            commitIndexListeners.forEach(listener -> listener.accept(commitIndex));
            // Note: Applying committed entries to the state machine could be added here.
        }
    }

    /**
     * Retrieves the log entry at the specified index.
     * @param index The index of the log entry.
     * @return The log entry, or null if the index is out of bounds.
     */
    public LogEntry getEntryAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index);
        }
        return null;
    }

    /**
     * Adds a listener to be notified when the commit index changes.
     * @param listener The listener to add, which accepts the new commit index.
     */
    public void addCommitIndexListener(Consumer<Integer> listener) {
        commitIndexListeners.add(listener);
    }

    /**
     * Removes a commit index listener.
     * @param listener The listener to remove.
     */
    public void removeCommitIndexListener(Consumer<Integer> listener) {
        commitIndexListeners.remove(listener);
    }
}
