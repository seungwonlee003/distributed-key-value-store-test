public enum ConsistencyLevel {
    DEFAULT,
    READ_YOUR_WRITES,
    LEADER_LOCAL,
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
