package com.example.raft.log;

import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Component
public class RaftLog {
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;
    private final File logFile = new File("raft_log.bin");
    private static final int INT_SIZE = Integer.BYTES; // 4 bytes

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

    private void writeEntryToDisk(int index, LogEntry entry) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            long position = channel.size();
            channel.position(position);
            ByteBuffer buffer = serializeEntry(index, entry);
            channel.write(buffer);
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entry to disk", e);
        }
    }

    private void writeEntriesToDisk(int startIndex, List<LogEntry> entries) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw");
             FileChannel channel = raf.getChannel()) {
            long position = channel.size();
            channel.position(position);
            for (int i = 0; i < entries.size(); i++) {
                ByteBuffer buffer = serializeEntry(startIndex + i, entries.get(i));
                channel.write(buffer);
            }
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append log entries to disk", e);
        }
    }

    private ByteBuffer serializeEntry(int index, LogEntry entry) {
        int term = entry.getTerm();
        int operationOrdinal = entry.getOperation().ordinal();
        String key = entry.getKey();
        String value = entry.getValue();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
        int valueLen = valueBytes != null ? valueBytes.length : -1;

        // Calculate the size for: index, term, operation, key length, key, value length, and optionally value
        int entrySize = INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + keyBytes.length + INT_SIZE;
        if (valueLen >= 0) {
            entrySize += valueLen;
        }

        ByteBuffer entryBuffer = ByteBuffer.allocate(entrySize);
        entryBuffer.putInt(index);
        entryBuffer.putInt(term);
        entryBuffer.putInt(operationOrdinal);
        entryBuffer.putInt(keyBytes.length);
        entryBuffer.put(keyBytes);
        entryBuffer.putInt(valueLen);
        if (valueLen >= 0) {
            entryBuffer.put(valueBytes);
        }
        entryBuffer.flip();
        return entryBuffer;
    }

    private void truncateLogFile(int fromIndex) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            long offset = 0;
            // Calculate the offset by iterating through the entries up to the given index
            for (int i = 0; i < fromIndex; i++) {
                LogEntry entry = logEntries.get(i);
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
                int valueLen = valueBytes != null ? valueBytes.length : -1;

                int entrySize = INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + keyBytes.length + INT_SIZE;
                if (valueLen >= 0) {
                    entrySize += valueLen;
                }
                offset += entrySize;
            }
            raf.setLength(offset);
            raf.getChannel().force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to truncate log file", e);
        }
    }
    
    private void recoverFromDisk() {
        logEntries.clear();
        if (!logFile.exists()) {
            return;
        }
    
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "r")) {
            int expectedIndex = 0;
            while (raf.getFilePointer() < raf.length()) {
                int index = raf.readInt();
                if (index != expectedIndex) {
                    throw new RuntimeException("Log index mismatch: expected " + expectedIndex + ", got " + index);
                }
                int term = raf.readInt();
                int opOrdinal = raf.readInt();
                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
    
                int valueLen = raf.readInt();
                byte[] valueBytes = null;
                if (valueLen >= 0) {
                    valueBytes = new byte[valueLen];
                    raf.readFully(valueBytes);
                }
    
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String value = (valueBytes != null) ? new String(valueBytes, StandardCharsets.UTF_8) : null;
    
                LogEntry entry = new LogEntry(term, key, value, LogEntry.Operation.values()[opOrdinal]);
                logEntries.add(entry);
    
                expectedIndex++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover log from disk", e);
        }
    }
}
