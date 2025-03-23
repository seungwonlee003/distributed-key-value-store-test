import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory implementation of LogStore using an ArrayList.
 */
public class InMemoryLogStore implements LogStore {
    private final List<LogEntry> logEntries = new ArrayList<>();

    public InMemoryLogStore() {
        logEntries.add(new LogEntry(0, null)); // Sentinel entry at index 0
    }

    @Override
    public void append(LogEntry entry) {
        logEntries.add(entry);
    }

    @Override
    public boolean containsEntryAt(int index) {
        return index >= 1 && index < logEntries.size();
    }

    @Override
    public int getTermAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index).getTerm();
        }
        return -1;
    }

    @Override
    public void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            logEntries.subList(fromIndex, logEntries.size()).clear();
        }
    }

    @Override
    public int getLastIndex() {
        return logEntries.size() - 1;
    }

    @Override
    public int getLastTerm() {
        if (logEntries.size() > 1) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        return 0;
    }

    @Override
    public LogEntry getEntryAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index);
        }
        return null;
    }

    @Override
    public List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= getLastIndex(); i++) {
            entries.add(getEntryAt(i));
        }
        return entries;
    }
}
