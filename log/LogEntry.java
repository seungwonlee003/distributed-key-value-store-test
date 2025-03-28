package com.example.raft.log;

public class LogEntry {
    private final int term;          // Raft term for this entry
    private final String key;        // Key for the operation
    private final String value;      // Value for INSERT/UPDATE (null for DELETE)
    private final Operation operation; // Enum for operation type

    public enum Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    public LogEntry(int term, String key, String value, Operation operation) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (operation == null) {
            throw new IllegalArgumentException("Operation cannot be null");
        }
        if ((operation == Operation.INSERT || operation == Operation.UPDATE) && value == null) {
            throw new IllegalArgumentException("Value cannot be null for " + operation);
        }
        this.term = term;
        this.key = key;
        this.value = value; // Null for DELETE
        this.operation = operation;
    }

    public int getTerm() { return term; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public Operation getOperation() { return operation; }

    @Override
    public String toString() {
        return "LogEntry{term=" + term + ", key='" + key + "', value='" + value + "', operation=" + operation + "}";
    }
}
