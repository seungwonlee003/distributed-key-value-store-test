package com.example.raft.log;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class InMemoryRaftLog implements RaftLog {

    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;

    /**
     * Initializes the log with a dummy entry if it is empty.
     */
    public InMemoryRaftLog() {
        if (logEntries.isEmpty()) {
            // The dummy entry is at index 0.
            LogEntry dummy = new LogEntry(0, "__dummy__", null, LogEntry.Operation.DELETE);
            logEntries.add(dummy);
        }
    }

    @Override
    public synchronized void append(LogEntry entry) {
        logEntries.add(entry);
    }

    @Override
    public synchronized void appendAll(List<LogEntry> entries) {
        if (entries != null && !entries.isEmpty()) {
            logEntries.addAll(entries);
        }
    }

    @Override
    public boolean containsEntryAt(int index) {
        // Assuming index 0 is reserved for a dummy entry.
        return index >= 1 && index < logEntries.size();
    }

    @Override
    public int getTermAt(int index) {
        LogEntry entry = getEntryAt(index);
        return entry != null ? entry.getTerm() : -1;
    }

    @Override
    public synchronized void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            // Remove all entries from the specified index onward.
            logEntries.subList(fromIndex, logEntries.size()).clear();
        }
    }

    @Override
    public int getLastIndex() {
        // Return the last index (accounting for the dummy entry at index 0)
        return logEntries.size() - 1;
    }

    @Override
    public int getLastTerm() {
        return logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getTerm();
    }

    @Override
    public int getCommitIndex() {
        return commitIndex;
    }

    @Override
    public synchronized void setCommitIndex(int newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastIndex()) {
            commitIndex = newCommitIndex;
        }
    }

    @Override
    public LogEntry getEntryAt(int index) {
        return (index >= 0 && index < logEntries.size()) ? logEntries.get(index) : null;
    }

    @Override
    public synchronized List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i < logEntries.size(); i++) {
            entries.add(logEntries.get(i));
        }
        return entries;
    }
}
