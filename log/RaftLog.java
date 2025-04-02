package com.example.raft.log;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class RaftLog {
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;
    private final File logFile = new File("raft_log.txt"); // Text-based log file
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public RaftLog() {
        recoverFromDisk();
        if (logEntries.isEmpty()) {
            LogEntry dummy = new LogEntry(0, "__dummy__", null, LogEntry.Operation.DELETE);
            append(dummy);
        }
    }

    // Core Log Operations
    public synchronized void append(LogEntry entry) {
        int index = logEntries.size();
        writeEntryToDisk(index, entry);
        logEntries.add(entry);
    }

    public synchronized void appendAll(List<LogEntry> entries) {
        if (entries != null && !entries.isEmpty()) {
            int startIndex = logEntries.size();
            writeEntriesToDisk(startIndex, entries);
            logEntries.addAll(entries);
        }
    }

    public boolean containsEntryAt(int index) {
        return index >= 1 && index < logEntries.size();
    }

    public int getTermAt(int index) {
        LogEntry entry = getEntryAt(index);
        return entry != null ? entry.getTerm() : -1;
    }

    public synchronized void deleteFrom(int fromIndex) {
        if (fromIndex >= 1 && fromIndex < logEntries.size()) {
            truncateLogFile(fromIndex);
            logEntries.subList(fromIndex, logEntries.size()).clear();
        }
    }

    public int getLastIndex() {
        return logEntries.size() - 1; // -1 if only dummy exists
    }

    public int getLastTerm() {
        return logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getTerm();
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(int newCommitIndex) {
        if (newCommitIndex > commitIndex && newCommitIndex <= getLastIndex()) {
            commitIndex = newCommitIndex;
        }
    }

    public LogEntry getEntryAt(int index) {
        return containsEntryAt(index) ? logEntries.get(index) : null;
    }

    public synchronized List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i < logEntries.size(); i++) {
            entries.add(logEntries.get(i));
        }
        return entries;
    }

    private void writeEntryToDisk(int index, LogEntry entry) {
        try (FileOutputStream fos = new FileOutputStream(logFile, true);
             FileChannel channel = fos.getChannel()) {
            String jsonLine = serializeEntry(index, entry);
            byte[] bytes = jsonLine.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            channel.write(buffer);
            channel.force(true); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entry to disk", e);
        }
    }

    private void writeEntriesToDisk(int startIndex, List<LogEntry> entries) {
        try (FileOutputStream fos = new FileOutputStream(logFile, true);
             FileChannel channel = fos.getChannel()) {
            for (int i = 0; i < entries.size(); i++) {
                int index = startIndex + i;
                String jsonLine = serializeEntry(index, entries.get(i));
                byte[] bytes = jsonLine.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                channel.write(buffer);
            }
            channel.force(true); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entries to disk", e);
        }
    }

    private String serializeEntry(int index, LogEntry entry) {
        try {
            Map<String, Object> data = new HashMap<>();
            data.put("index", index);
            data.put("term", entry.getTerm());
            data.put("operation", entry.getOperation().name());
            data.put("key", entry.getKey());
            data.put("value", entry.getValue());
            return mapper.writeValueAsString(data) + "\n"; // JSON Lines format
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize log entry to JSON", e);
        }
    }

    private void truncateLogFile(int fromIndex) {
        List<LogEntry> entriesToKeep = logEntries.subList(0, fromIndex);
        try (FileOutputStream fos = new FileOutputStream(logFile);
             FileChannel channel = fos.getChannel()) {
            for (int i = 0; i < entriesToKeep.size(); i++) {
                String jsonLine = serializeEntry(i, entriesToKeep.get(i));
                byte[] bytes = jsonLine.getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                channel.write(buffer);
            }
            channel.force(true); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate log file", e);
        }
    }

    // {"index":0,"term":0,"operation":"DELETE","key":"__dummy__","value":null}
    private void recoverFromDisk() {
        logEntries.clear();
        if (logFile.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(logFile, StandardCharsets.UTF_8))) {
                String line;
                int expectedIndex = 0;
                while ((line = br.readLine()) != null) {
                    try {
                        Map<String, Object> data = mapper.readValue(line, Map.class);
                        int index = ((Number) data.get("index")).intValue();
                        if (index != expectedIndex) {
                            throw new RuntimeException("Log index mismatch: expected " + expectedIndex + ", got " + index);
                        }
                        int term = ((Number) data.get("term")).intValue();
                        String operationStr = (String) data.get("operation");
                        LogEntry.Operation operation = LogEntry.Operation.valueOf(operationStr);
                        String key = (String) data.get("key");
                        String value = data.containsKey("value") ? (String) data.get("value") : null;
                        LogEntry entry = new LogEntry(term, key, value, operation);
                        logEntries.add(entry);
                        expectedIndex++;
                    } catch (JsonProcessingException e) {
                        // Stop recovery on partial/malformed line
                        break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to recover log from disk", e);
            }
        }
    }
}
