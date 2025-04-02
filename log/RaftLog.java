package com.example.raft.log;

import org.springframework.stereotype.Component;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

@Component
public class RaftLog {
    private final List<LogEntry> logEntries = new ArrayList<>();
    private int commitIndex = 0;
    private final File logFile = new File("raft_log.bin");
    private static final int INT_SIZE = Integer.BYTES; // 4 bytes
    private static final int MAGIC_HEADER = 0x52414654; // "RAFT" in ASCII
    private static final byte VERSION = 1;
    private static final int HEADER_SIZE = 5; // 4 bytes magic + 1 byte version

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
            if (position == 0) {
                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                header.putInt(MAGIC_HEADER);
                header.put(VERSION);
                header.flip();
                channel.write(header);
            }
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
            if (position == 0) {
                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                header.putInt(MAGIC_HEADER);
                header.put(VERSION);
                header.flip();
                channel.write(header);
            }
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

        CRC32 crc = new CRC32();
        crc.update(entryBuffer.array(), 0, entryBuffer.limit());
        int checksum = (int) crc.getValue();

        ByteBuffer buffer = ByteBuffer.allocate(entrySize + INT_SIZE);
        buffer.put(entryBuffer);
        buffer.putInt(checksum);
        buffer.flip();
        return buffer;
    }

    private void truncateLogFile(int fromIndex) {
        try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw")) {
            // Start with the header size (magic + version)
            long offset = HEADER_SIZE;

            // Calculate offset for entries up to fromIndex
            for (int i = 0; i < fromIndex; i++) {
                LogEntry entry = logEntries.get(i);
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value != null ? value.getBytes(StandardCharsets.UTF_8) : null;
                int valueLen = valueBytes != null ? valueBytes.length : -1;

                // Size: index + term + op + keyLen + key + valueLen + value + checksum
                int entrySize = INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + keyBytes.length + INT_SIZE;
                if (valueLen >= 0) {
                    entrySize += valueLen;
                }
                entrySize += INT_SIZE; // Add checksum size
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
            if (raf.length() < HEADER_SIZE) {
                // Not enough bytes to even have the header, treat as empty
                return;
            }
            int magic = raf.readInt();
            if (magic != MAGIC_HEADER) {
                throw new RuntimeException("Invalid magic header");
            }
            byte version = raf.readByte();
            if (version != VERSION) {
                throw new RuntimeException("Unsupported log version: " + version);
            }
    
            int expectedIndex = 0;
            while (raf.getFilePointer() < raf.length()) {
                long startPos = raf.getFilePointer();
    
                // Read the fields in the same order they were written
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
    
                // Finally, read the checksum (4 bytes)
                int storedChecksum = raf.readInt();
    
                // Figure out how many bytes total this entry (including checksum) occupied
                long endPos = raf.getFilePointer();
                int totalSize = (int) (endPos - startPos);
    
                // Rewind to startPos to read all but the last 4 checksum bytes for CRC verification
                raf.seek(startPos);
                byte[] entryData = new byte[totalSize - 4]; // exclude the checksum
                raf.readFully(entryData);
    
                // Compute our own CRC
                CRC32 crc = new CRC32();
                crc.update(entryData);
                int computedChecksum = (int) crc.getValue();
    
                // Compare
                if (computedChecksum != storedChecksum) {
                    // Corrupt entry: truncate and stop
                    raf.seek(startPos);
                    raf.setLength(startPos);
                    break;
                }
    
                // If CRC matches, move the pointer back to the end of this entry and add it to memory
                raf.seek(endPos);
    
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String value = (valueBytes != null) ? new String(valueBytes, StandardCharsets.UTF_8) : null;
    
                // Reconstruct the LogEntry using your constructor/enum
                LogEntry entry = new LogEntry(term, key, value, LogEntry.Operation.values()[opOrdinal]);
                logEntries.add(entry);
    
                expectedIndex++;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover log from disk", e);
        }
    }
}
