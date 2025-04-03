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
    // Non-volatile state
    private final int nodeId;
    private int currentTerm = 0;
    private Integer votedFor = null;
    private int lastApplied = 0;

    // Volatile state
    private Role currentRole = Role.FOLLOWER;
    private Integer currentLeader = null;

    private final File stateFile = new File("raft_node_state.bin");
    private static final int MAGIC_HEADER = 0x524E5354; // "RNST" in ASCII
    private static final byte VERSION = 1;
    private static final int INT_SIZE = Integer.BYTES; // 4 bytes
    private static final int HEADER_SIZE = 5; // 4 bytes magic + 1 byte version

    public RaftNodeState(RaftConfig config) {
        this.nodeId = config.getNodeId();
        this.config = config;
        recoverFromDisk();
    }

    @PostConstruct
    private void init() {
        if (!stateFile.exists()) {
            persistToDisk();
        }
    }

    public void setCurrentTerm(int term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
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
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
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
    
            byte[] payload = baos.toByteArray();
            int checksum = calculateChecksum(payload);
    
            try (DataOutputStream fileDos = new DataOutputStream(new FileOutputStream(stateFile))) {
                fileDos.write(payload);
                fileDos.writeInt(checksum);
                fileDos.flush();
                fileDos.getFD().sync(); // Ensure durability
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist RaftNodeState to disk", e);
        }
    }
    
    private void recoverFromDisk() {
        if(!stateFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(stateFile))) {
            long fileSize = stateFile.length();
            if (fileSize < HEADER_SIZE + INT_SIZE * 3 + 1 + INT_SIZE) {
                return;
            }

            // Read and store checksum first
            dis.mark((int) fileSize);
            dis.skipBytes((int) fileSize - INT_SIZE);
            int storedChecksum = dis.readInt();
            dis.reset();

            // Read payload
            byte[] payload = new byte[(int) fileSize - INT_SIZE];
            dis.readFully(payload);

            // Verify checksum
            if (calculateChecksum(payload) != storedChecksum) {
                throw new RuntimeException("Checksum mismatch; state file corrupted");
            }

            // Parse validated payload
            try (DataInputStream validatedDis = new DataInputStream(new ByteArrayInputStream(payload))) {
                // Verify header
                int magic = validatedDis.readInt();
                if (magic != MAGIC_HEADER) {
                    throw new RuntimeException("Invalid magic header");
                }
                byte version = validatedDis.readByte();
                if (version != VERSION) {
                    throw new RuntimeException("Unsupported version: " + version);
                }

                // Read and apply state data
                int storedNodeId = validatedDis.readInt();
                int term = validatedDis.readInt();
                boolean hasVotedFor = validatedDis.readBoolean();
                Integer voted = hasVotedFor ? validatedDis.readInt() : null;
                int applied = validatedDis.readInt();

                if (storedNodeId != nodeId) {
                    throw new RuntimeException("Node ID mismatch: expected " + nodeId + ", got " + storedNodeId);
                }
                currentTerm = term;
                votedFor = voted;
                lastApplied = applied;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover RaftNodeState from disk", e);
        }
    }
    
    private int calculateChecksum(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }
}
