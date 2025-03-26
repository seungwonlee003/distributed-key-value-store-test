public interface KVStore {
    void put(String key, String value);    // For INSERT and UPDATE
    void remove(String key);               // For DELETE
    String get(String key);                // For reads
    boolean containsKey(String key);       // Optional utility
}
