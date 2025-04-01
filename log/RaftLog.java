import java.io.*;
import java.util.*;
import org.springframework.stereotype.Component;

@Component
public class RaftLog {
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;
    private static final String LOG_FILE_PATH = "raft_log.txt"; 

    public RaftLog() {
        loadLogFromFile();
    }

    public synchronized void append(LogEntry entry) {
        logEntries.add(entry);
        appendLogEntryToFile(entry);
    }

    public synchronized void appendAll(List<LogEntry> entries) {
        if (entries != null && !entries.isEmpty()) {
            logEntries.addAll(entries);
            appendMultipleEntriesToFile(entries);
        }
    }

    public synchronized boolean containsEntryAt(int index) {
        return index >= 1 && index < logEntries.size();
    }

    public synchronized int getTermAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index).getTerm();
        }
        return -1; // Invalid term
    }

    // Delete log entries starting from the given index.
    // This requires rewriting the entire file.
    public synchronized void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            logEntries.subList(fromIndex, logEntries.size()).clear();
            rewriteLogFile();
        }
    }

    public synchronized int getLastIndex() {
        return logEntries.size() - 1;
    }

    public synchronized int getLastTerm() {
        if (logEntries.size() > 1) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        return 0;
    }

    public synchronized int getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(int newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastIndex()) {
            commitIndex = newCommitIndex;
        }
    }

    public synchronized LogEntry getEntryAt(int index) {
        if (containsEntryAt(index)) {
            return logEntries.get(index);
        }
        return null;
    }

    public synchronized List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i < logEntries.size(); i++) {
            entries.add(logEntries.get(i));
        }
        return entries;
    }

    // Load log entries from disk at startup.
    private synchronized void loadLogFromFile() {
        File file = new File(LOG_FILE_PATH);
        if (!file.exists()) {
            System.out.println("No previous log file found, starting fresh.");
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    logEntries.add(parseLogEntry(line));
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading log from file: " + e.getMessage());
        }
    }

    private synchronized void appendLogEntryToFile(LogEntry entry) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            writer.write(entryToString(entry));
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error appending log entry to file: " + e.getMessage());
        }
    }

    private synchronized void appendMultipleEntriesToFile(List<LogEntry> entries) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            for (LogEntry entry : entries) {
                writer.write(entryToString(entry));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error appending multiple log entries to file: " + e.getMessage());
        }
    }

    // Rewrite the entire log file from the current in-memory log entries.
    private synchronized void rewriteLogFile() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH))) {
            for (LogEntry entry : logEntries) {
                writer.write(entryToString(entry));
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error rewriting log file: " + e.getMessage());
        }
    }

    private String entryToString(LogEntry entry) {
        return entry.getTerm() + "," + entry.getKey() + "," + entry.getValue() + "," + entry.getOperation();
    }

    // Parse a CSV-formatted string into a LogEntry.
    private LogEntry parseLogEntry(String entryStr) {
        String[] parts = entryStr.split(",");
        int term = Integer.parseInt(parts[0]);
        String key = parts[1];
        String value = parts[2];
        LogEntry.Operation operation = LogEntry.Operation.valueOf(parts[3]);
        return new LogEntry(term, key, value, operation);
    }
}
