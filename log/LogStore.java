import java.util.List;

/**
 * Interface for storing and managing Raft log entries.
 * Implementations can be in-memory, disk-based, etc.
 */
public interface LogStore {
    /** Append a new log entry to the store. */
    void append(LogEntry entry);

    /** Check if an entry exists at the given index. */
    boolean containsEntryAt(int index);

    /** Get the term of the entry at the given index, or -1 if invalid. */
    int getTermAt(int index);

    /** Delete all entries from the given index to the end. */
    void deleteFrom(int fromIndex);

    /** Get the index of the last entry in the log. */
    int getLastIndex();

    /** Get the term of the last entry, or 0 if the log is empty (beyond sentinel). */
    int getLastTerm();

    /** Get the entry at the specified index, or null if it doesn’t exist. */
    LogEntry getEntryAt(int index);

    /** Get a list of entries from startIndex to endIndex (inclusive), up to the last index. */
    List<LogEntry> getEntriesFrom(int startIndex, int endIndex);
}
