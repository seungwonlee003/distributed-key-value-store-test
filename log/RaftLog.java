package com.example.raft.log;

import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

@Component
public class RaftLog {
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;
    private final File logFile = new File("raft_log.bin"); 

    public RaftLog() {
        recoverFromDisk();
        if (logEntries.isEmpty()) {
            LogEntry dummy = new LogEntry(0, "__dummy__", null, LogEntry.Operation.DELETE);
            append(dummy);
        }
    }

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

    // Disk I/O Methods

    /**
     * Writes a single entry to disk with its index.
     */
    private void writeEntryToDisk(int index, LogEntry entry) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            channel.position(channel.size());
            ByteBuffer buffer = serializeEntry(index, entry);
            channel.write(buffer);
            channel.force(true); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entry to disk", e);
        }
    }

    /**
     * Writes multiple entries to disk with consecutive indices.
     */
    private void writeEntriesToDisk(int startIndex, List<LogEntry> entries) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            channel.position(channel.size());
            for (int i = 0; i < entries.size(); i++) {
                int index = startIndex + i;
                ByteBuffer buffer = serializeEntry(index, entries.get(i));
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entries to disk", e);
        }
    }

    /**
     * Serializes an entry with its index into a ByteBuffer.
     */
    private ByteBuffer serializeEntry(int index, LogEntry entry) {
        int term = entry.getTerm();
        int operationOrdinal = entry.getOperation().ordinal();
        String key = entry.getKey();
        String value = entry.getValue();
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value != null ? value.getBytes() : null;
        int valueLen = valueBytes != null ? valueBytes.length : -1;

        // Calculate size: index (4), term (4), operation (4), keyLen (4), key, valueLen (4), value (if present)
        int totalSize = 4 + 4 + 4 + 4 + keyBytes.length + 4;
        if (valueLen >= 0) {
            totalSize += valueLen;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(index);
        buffer.putInt(term);
        buffer.putInt(operationOrdinal);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueLen);
        if (valueLen >= 0) {
            buffer.put(valueBytes);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Truncates the log file starting from the specified index.
     */
    private void truncateLogFile(int fromIndex) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            long offset = 0;
            for (int i = 0; i < fromIndex; i++) {
                LogEntry entry = logEntries.get(i);
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes();
                byte[] valueBytes = value != null ? value.getBytes() : null;
                int valueLen = valueBytes != null ? valueBytes.length : -1;
                offset += 4 + 4 + 4 + 4 + keyBytes.length + 4; // index, term, op, keyLen, key, valueLen
                if (valueLen >= 0) {
                    offset += valueLen;
                }
            }
            raf.setLength(offset);
            raf.getChannel().force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate log file", e);
        }
    }

    /**
     * Recovers the log from disk, ensuring indices match positions.
     */
    private void recoverFromDisk() {
        logEntries.clear();
        if (logFile.exists()) {
            try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
                int expectedIndex = 0;
                while (raf.getFilePointer() < raf.length()) {
                    int index = raf.readInt();
                    if (index != expectedIndex) {
                        throw new RuntimeException("Log index mismatch: expected " + expectedIndex + ", got " + index);
                    }
                    int term = raf.readInt();
                    int opOrdinal = raf.readInt();
                    LogEntry.Operation operation = LogEntry.Operation.values()[opOrdinal];
                    int keyLen = raf.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    raf.readFully(keyBytes);
                    String key = new String(keyBytes);
                    int valueLen = raf.readInt();
                    String value = null;
                    if (valueLen >= 0) {
                        byte[] valueBytes = new byte[valueLen];
                        raf.readFully(valueBytes);
                        value = new String(valueBytes);
                    }
                    LogEntry entry = new LogEntry(term, key, value, operation);
                    logEntries.add(entry);
                    expectedIndex++;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to recover log from disk", e);
            }
        }
    }
}
