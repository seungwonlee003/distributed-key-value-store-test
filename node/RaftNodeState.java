package com.example.raft;

import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.*;
import java.util.Objects;
import java.util.zip.CRC32;

@Getter
@Setter
@Component
public class RaftNodeState {
    private RaftConfig config;
    // Non-volatile state (persisted)
    private final int nodeId;
    private int currentTerm = 0;
    private Integer votedFor = null;
    private int lastApplied = 0;

    // Volatile state (not persisted)
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;

    private final File stateFile = new File("raft_node_state.bin");
    private static final int MAGIC_HEADER = 0x524E5354; // "RNST" in ASCII
    private static final byte VERSION = 1;
    private static final int INT_SIZE = Integer.BYTES; // 4 bytes
    private static final int HEADER_SIZE = 5; // 4 bytes magic + 1 byte version

    public RaftNodeState(int nodeId, RaftConfig config) {
        this.nodeId = nodeId;
        this.config = config;
        recoverFromDisk();
    }

    @PostConstruct
    private void init() {
        if (!stateFile.exists()) {
            persistToDisk(); // Write initial state if no file exists
        }
    }

    public void setCurrentTerm(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null; // Reset vote when term increases
            persistToDisk();
        }
    }

    public void setVotedFor(Integer votedFor) {
        this.votedFor = votedFor;
        persistToDisk();
    }

    public void setLastApplied(int lastApplied) {
        if (lastApplied > this.lastApplied) {
            this.lastApplied = lastApplied;
            persistToDisk();
        }
    }

    public void setCurrentLeader(Integer leaderId) {
        if (!Objects.equals(this.currentLeader, leaderId)) {
            System.out.println("New leader detected: Node " + leaderId);
        }
        this.currentLeader = leaderId;
    }

    public String getCurrentLeaderUrl() {
        return config.getPeerUrls().get(currentLeader);
    }

    public boolean isLeader() {
        return currentRole.equals(Role.LEADER);
    }

    private synchronized void persistToDisk() {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(stateFile))) {
            // Write header
            dos.writeInt(MAGIC_HEADER);
            dos.writeByte(VERSION);

            // Write state data
            dos.writeInt(nodeId);
            dos.writeInt(currentTerm);
            dos.writeBoolean(votedFor != null);
            if (votedFor != null) {
                dos.writeInt(votedFor);
            }
            dos.writeInt(lastApplied);

            // Compute and write checksum
            byte[] data = ((ByteArrayOutputStream) dos.getUnderlyingOutputStream()).toByteArray();
            CRC32 crc = new CRC32();
            crc.update(data, 0, data.length - INT_SIZE); // Exclude checksum itself
            dos.writeInt((int) crc.getValue()); // 4 bytes

            dos.flush();
            dos.getFD().sync(); // Ensure durability
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist RaftNodeState to disk", e);
        }
    }

    private void recoverFromDisk() {
        if (stateFile.exists()) {
            try (DataInputStream dis = new DataInputStream(new FileInputStream(stateFile))) {
                if (dis.available() < HEADER_SIZE) {
                    return; // File too small to be valid
                }

                // Verify header
                int magic = dis.readInt();
                if (magic != MAGIC_HEADER) {
                    throw new RuntimeException("Invalid magic header");
                }
                byte version = dis.readByte();
                if (version != VERSION) {
                    throw new RuntimeException("Unsupported version: " + version);
                }

                // Read state data
                int storedNodeId = dis.readInt();
                int term = dis.readInt();
                boolean hasVotedFor = dis.readBoolean();
                Integer voted = hasVotedFor ? dis.readInt() : null;
                int applied = dis.readInt();
                int storedChecksum = dis.readInt();

                // Verify checksum
                byte[] data = new byte[(int) (stateFile.length() - INT_SIZE)];
                try (FileInputStream fis = new FileInputStream(stateFile)) {
                    fis.read(data);
                }
                CRC32 crc = new CRC32();
                crc.update(data, 0, data.length - INT_SIZE); // Exclude checksum
                if ((int) crc.getValue() != storedChecksum) {
                    throw new RuntimeException("Checksum mismatch; state file corrupted");
                }

                // Apply state if valid
                if (storedNodeId != nodeId) {
                    throw new RuntimeException("Node ID mismatch: expected " + nodeId + ", got " + storedNodeId);
                }
                currentTerm = term;
                votedFor = voted;
                lastApplied = applied;
            } catch (IOException e) {
                throw new RuntimeException("Failed to recover RaftNodeState from disk", e);
            }
        }
    }
}
