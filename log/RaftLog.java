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
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;

    public RaftLog() {
        logEntries.add(new LogEntry(0, null)); // Term 0, no command
    }

    public void append(LogEntry entry) {
        logEntries.add(entry);
    }

    public void appendAll(List<LogEntry> entries) {
        if (entries != null && !entries.isEmpty()) {
            logEntries.addAll(entries); // Bulk append all entries
        }
    }

    public boolean containsEntryAt(int index) {
        return index >= 1 && index < logEntries.size();
    }

    public int getTermAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index).getTerm();
        }
        return -1; // Invalid term
    }

    public void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            logEntries.subList(fromIndex, logEntries.size()).clear();
        }
    }

    public int getLastIndex() {
        return logEntries.size() - 1;
    }

 
    public int getLastTerm() {
        if (logEntries.size() > 1) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        return 0;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastIndex()) {
            commitIndex = newCommitIndex;
        }
    }

    public LogEntry getEntryAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index);
        }
        return null;
    }
    
    public synchronized List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= getLastIndex(); i++) {
            entries.add(getEntryAt(i));
        }
        return entries;
    }
}
