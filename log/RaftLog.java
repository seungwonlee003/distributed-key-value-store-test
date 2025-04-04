package com.example.raft.log;

import java.util.List;

public interface RaftLog {

    void append(LogEntry entry);

    void appendAll(List<LogEntry> entries);

    boolean containsEntryAt(int index);

    int getTermAt(int index);

    void deleteFrom(int fromIndex);

    int getLastIndex();

    int getLastTerm();

    int getCommitIndex();

    void setCommitIndex(int newCommitIndex);

    LogEntry getEntryAt(int index);

    List<LogEntry> getEntriesFrom(int startIndex, int endIndex);
}
