import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;

@Service
public class RaftLogManager {
    private final RaftNode raftNode;
    private final RaftLog raftLog;
    private final RaftNodeState raftNodeState;
    private final Map<String, Integer> nextIndex;  // Tracks next log index to send to each follower.
    private final Map<String, Integer> matchIndex; // Tracks highest replicated log index per follower.

    public RaftLogManager(RaftNode raftNode, RaftLog raftLog) {
        this.raftNode = raftNode;
        this.raftLog = raftLog;
        this.raftNodeState = raftNode.getState();
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
    }

    // Initialize replication indices when the node becomes leader (Raft Section 5.3).
    @PostConstruct
    public void initialize() {
        if (raftNodeState.getRole() == Role.LEADER) {
            initializeIndices();
        }
    }

    private void initializeIndices() {
        int lastLogIndex = raftLog.getLastIndex();
        for (String peerUrl : raftNode.getPeerUrls()) {
            nextIndex.put(peerUrl, lastLogIndex + 1);
            matchIndex.put(peerUrl, 0);
        }
    }

    // Handles incoming AppendEntries RPCs (heartbeats or log replication) as a follower (Raft Section 5.3).
    public synchronized AppendEntryResponseDTO handleAppendEntries(AppendEntryDTO dto) {
        int currentTerm = raftNodeState.getCurrentTerm();
        int leaderTerm = dto.getTerm();
        int prevLogIndex = dto.getPrevLogIndex();
        int prevLogTerm = dto.getPrevLogTerm();
        List<LogEntry> entries = dto.getEntries();
        int leaderCommit = dto.getLeaderCommit();

        // Reject if leader’s term is outdated.
        if (leaderTerm < currentTerm) {
            return new AppendEntryResponseDTO(currentTerm, false, raftLog.getLastIndex());
        }

        // Step down if a higher term is observed.
        if (leaderTerm > currentTerm) {
            raftNodeState.setCurrentTerm(leaderTerm);
            raftNodeState.setRole(Role.FOLLOWER);
            raftNodeState.setVotedFor(null);
            raftNode.resetElectionTimer();
            currentTerm = leaderTerm;
        }

        // Log consistency check: Ensure prevLogIndex and prevLogTerm match local log.
        if (prevLogIndex > 0 && (!raftLog.containsEntryAt(prevLogIndex) || raftLog.getTermAt(prevLogIndex) != prevLogTerm)) {
            int lastMatchingIndex = findLastMatchingIndex(prevLogIndex);
            return new AppendEntryResponseDTO(currentTerm, false, lastMatchingIndex);
        }

        // Append new entries, resolving conflicts by overwriting (Raft log matching property).
        int index = prevLogIndex + 1;
        for (LogEntry entry : entries) {
            if (raftLog.containsEntryAt(index) && raftLog.getTermAt(index) != entry.getTerm()) {
                raftLog.deleteFrom(index); // Overwrite conflicting entries.
            }
            if (!raftLog.containsEntryAt(index)) {
                raftLog.append(entry);
            }
            index++;
        }

        // Update commit index based on leader’s commit index (Raft Section 5.3).
        if (leaderCommit > raftLog.getCommitIndex()) {
            int lastNewEntryIndex = prevLogIndex + entries.size();
            raftLog.setCommitIndex(Math.min(leaderCommit, lastNewEntryIndex));
        }

        // Reset election timer since we’ve heard from the leader.
        raftNode.resetElectionTimer();
        return new AppendEntryResponseDTO(currentTerm, true, raftLog.getLastIndex());
    }

    // Leader-only: Sends heartbeats to followers (Raft Section 5.2).
    public void sendHeartbeatToFollowers() {
        if (raftNodeState.getRole() != Role.LEADER) return; // Only leader broadcasts.

        int currentTerm = raftNodeState.getCurrentTerm();
        int lastLogIndex = raftLog.getLastIndex();
        int prevLogIndex = lastLogIndex;
        int prevLogTerm = lastLogIndex > 0 ? raftLog.getTermAt(lastLogIndex) : 0;
        int commitIndex = raftLog.getCommitIndex();

        AppendEntryDTO heartbeatDto = new AppendEntryDTO(
            currentTerm, raftNodeState.getNodeId(), prevLogIndex, prevLogTerm,
            Collections.emptyList(), commitIndex
        );

        for (String peerUrl : raftNode.getPeerUrls()) {
            sendAppendEntriesAsync(peerUrl, heartbeatDto);
        }
    }

    // Leader-only: Replicates a new log entry to followers (Raft Section 5.3).
    public void replicateLogEntryToFollowers(LogEntry entry) {
        if (raftNodeState.getRole() != Role.LEADER) return; // Only leader initiates replication.

        raftLog.append(entry); // Leader appends to its own log first.
        int currentTerm = raftNodeState.getCurrentTerm();
        int lastLogIndex = raftLog.getLastIndex();

        if (nextIndex.isEmpty()) {
            initializeIndices(); // Ensure indices are initialized for new leader.
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (String peerUrl : raftNode.getPeerUrls()) {
            futures.add(replicateToFollower(peerUrl, currentTerm, lastLogIndex));
        }

        // Commit when majority replicates (Raft Section 5.4).
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).thenRun(() -> {
            int replicatedCount = 1; // Leader’s own log counts.
            for (String peerUrl : raftNode.getPeerUrls()) {
                if (matchIndex.getOrDefault(peerUrl, 0) >= lastLogIndex) {
                    replicatedCount++;
                }
            }
            int majority = (raftNode.getPeerUrls().size() + 1) / 2 + 1;
            if (replicatedCount >= majority && raftLog.getTermAt(lastLogIndex) == currentTerm) {
                raftLog.setCommitIndex(lastLogIndex); // Commit only if in current term.
            }
        });
    }

    // Replicates log entries to a single follower, adjusting nextIndex on failure (Raft Section 5.3).
    private CompletableFuture<Void> replicateToFollower(String peerUrl, int currentTerm, int targetIndex) {
        return CompletableFuture.runAsync(() -> {
            while (raftNodeState.getRole() == Role.LEADER && nextIndex.get(peerUrl) <= targetIndex) {
                int ni = nextIndex.get(peerUrl);
                int prevLogIndex = Math.max(0, ni - 1); // Avoid negative index.
                int prevLogTerm = prevLogIndex > 0 ? raftLog.getTermAt(prevLogIndex) : 0;
                List<LogEntry> entriesToSend = getEntriesFrom(ni, targetIndex);

                AppendEntryDTO dto = new AppendEntryDTO(
                    currentTerm, raftNodeState.getNodeId(), prevLogIndex, prevLogTerm,
                    entriesToSend, raftLog.getCommitIndex()
                );

                AppendEntryResponseDTO response = sendAppendEntriesAsync(peerUrl, dto).join();
                synchronized (this) {
                    if (response.getTerm() > currentTerm) {
                        raftNodeState.setCurrentTerm(response.getTerm());
                        raftNodeState.setRole(Role.FOLLOWER);
                        raftNodeState.setVotedFor(null);
                        raftNode.resetElectionTimer();
                        return;
                    }
                    if (response.isSuccess()) {
                        nextIndex.put(peerUrl, ni + entriesToSend.size());
                        matchIndex.put(peerUrl, ni + entriesToSend.size() - 1);
                    } else {
                        int lastMatchingIndex = response.getLastMatchingIndex();
                        nextIndex.put(peerUrl, Math.max(1, lastMatchingIndex + 1));
                    }
                }
            }
        }, raftNode.getAsyncExecutor());
    }

    // Sends AppendEntries RPC asynchronously to a peer.
    private CompletableFuture<AppendEntryResponseDTO> sendAppendEntriesAsync(String peerUrl, AppendEntryDTO dto) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String url = peerUrl + "/raft/appendEntries";
                ResponseEntity<AppendEntryResponseDTO> response = raftNode.getRestTemplate()
                    .postForEntity(url, dto, AppendEntryResponseDTO.class);
                return response.getBody() != null ? response.getBody() : new AppendEntryResponseDTO(-1, false, -1);
            } catch (Exception e) {
                return new AppendEntryResponseDTO(-1, false, -1); // Indicate failure on network error.
            }
        }, raftNode.getAsyncExecutor());
    }

    // Finds the last matching log index when consistency check fails.
    private int findLastMatchingIndex(int prevLogIndex) {
        for (int i = Math.min(prevLogIndex, raftLog.getLastIndex()); i >= 1; i--) {
            if (raftLog.containsEntryAt(i)) {
                return i;
            }
        }
        return 0;
    }

    // Retrieves log entries from startIndex to endIndex (inclusive).
    private List<LogEntry> getEntriesFrom(int startIndex, int endIndex) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = startIndex; i <= endIndex && i <= raftLog.getLastIndex(); i++) {
            entries.add(raftLog.getEntryAt(i));
        }
        return entries;
    }
}
