public enum ConsistencyLevel {
    DEFAULT,
    LINEARIZABLE, // read_your_writes
    EVENTUAL;

    public static ConsistencyLevel fromString(String value) {
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid consistency level: " + value + 
                ". Valid options: STRONG, LEADER_LOCAL, EVENTUAL");
        }
    }
}
