import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LogReplicatorTest {
    @Mock
    private RaftConfig config;

    @Mock
    private RaftLog log;

    @Mock
    private RaftStateManager stateManager;

    @Mock
    private RaftNodeState nodeState;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private StateMachine stateMachine;

    @Mock
    private ScheduledExecutorService executor;

    @Mock
    private ExecutorService applyExecutor;

    @InjectMocks
    private LogReplicator logReplicator;

    private List<String> peerUrls;

    @BeforeEach
    void setUp() {
        peerUrls = Arrays.asList("http://peer1:8080", "http://peer2:8080");
        when(config.getPeerUrlList()).thenReturn(peerUrls);
        when(config.getHeartbeatIntervalMillis()).thenReturn(100);
        when(nodeState.getNodeId()).thenReturn(1);
        when(nodeState.getCurrentTerm()).thenReturn(2);
        when(log.getLastIndex()).thenReturn(5);

        // Initialize executor manually since @PostConstruct isn't called automatically in tests
        logReplicator.initExecutor();
    }

    @Test
    void initializeIndices_setsNextAndMatchIndices() {
        logReplicator.initializeIndices();

        assertEquals(6, logReplicator.nextIndex.get("http://peer1:8080"));
        assertEquals(6, logReplicator.nextIndex.get("http://peer2:8080"));
        assertEquals(0, logReplicator.matchIndex.get("http://peer1:8080"));
        assertEquals(0, logReplicator.matchIndex.get("http://peer2:8080"));
    }

    @Test
    void start_whenNotLeader_doesNothing() {
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);

        logReplicator.start();

        verify(executor, never()).submit(any(Runnable.class));
    }

    @Test
    void start_whenLeader_submitsReplicationTasks() {
        when(nodeState.getRole()).thenReturn(Role.LEADER);

        logReplicator.start();

        verify(executor, times(2)).submit(any(Runnable.class));
        assertTrue(logReplicator.pendingReplication.get("http://peer1:8080"));
        assertTrue(logReplicator.pendingReplication.get("http://peer2:8080"));
    }

    @Test
    void replicate_success_updatesIndices() {
        String peer = "http://peer1:8080";
        logReplicator.nextIndex.put(peer, 5);
        when(nodeState.getRole()).thenReturn(Role.LEADER);
        when(log.getTermAt(4)).thenReturn(1);
        List<LogEntry> entries = Arrays.asList(new LogEntry(2, null));
        when(log.getEntriesFrom(5)).thenReturn(entries);
        AppendEntryResponseDTO response = new AppendEntryResponseDTO(2, true);
        when(restTemplate.postForEntity(eq(peer + "/raft/appendEntries"), any(), eq(AppendEntryResponseDTO.class)))
                .thenReturn(ResponseEntity.ok(response));

        boolean result = logReplicator.replicate(peer);

        assertTrue(result);
        assertEquals(6, logReplicator.nextIndex.get(peer));
        assertEquals(5, logReplicator.matchIndex.get(peer));
    }

    @Test
    void replicate_higherTerm_becomesFollower() {
        String peer = "http://peer1:8080";
        logReplicator.nextIndex.put(peer, 5);
        when(nodeState.getRole()).thenReturn(Role.LEADER);
        when(log.getTermAt(4)).thenReturn(1);
        when(log.getEntriesFrom(5)).thenReturn(Arrays.asList(new LogEntry(2, null)));
        AppendEntryResponseDTO response = new AppendEntryResponseDTO(3, false);
        when(restTemplate.postForEntity(eq(peer + "/raft/appendEntries"), any(), eq(AppendEntryResponseDTO.class)))
                .thenReturn(ResponseEntity.ok(response));

        boolean result = logReplicator.replicate(peer);

        assertFalse(result);
        verify(stateManager).becomeFollower(3);
    }

    @Test
    void replicate_failure_decrementsNextIndex() {
        String peer = "http://peer1:8080";
        logReplicator.nextIndex.put(peer, 5);
        when(nodeState.getRole()).thenReturn(Role.LEADER);
        when(log.getTermAt(4)).thenReturn(1);
        when(log.getEntriesFrom(5)).thenReturn(Arrays.asList(new LogEntry(2, null)));
        AppendEntryResponseDTO response = new AppendEntryResponseDTO(2, false);
        when(restTemplate.postForEntity(eq(peer + "/raft/appendEntries"), any(), eq(AppendEntryResponseDTO.class)))
                .thenReturn(ResponseEntity.ok(response));

        boolean result = logReplicator.replicate(peer);

        assertFalse(result);
        assertEquals(4, logReplicator.nextIndex.get(peer));
    }

    @Test
    void replicate_exception_returnsFalse() {
        String peer = "http://peer1:8080";
        logReplicator.nextIndex.put(peer, 5);
        when(nodeState.getRole()).thenReturn(Role.LEADER);
        when(log.getTermAt(4)).thenReturn(1);
        when(log.getEntriesFrom(5)).thenReturn(Arrays.asList(new LogEntry(2, null)));
        when(restTemplate.postForEntity(eq(peer + "/raft/appendEntries"), any(), eq(AppendEntryResponseDTO.class)))
                .thenThrow(new RuntimeException());

        boolean result = logReplicator.replicate(peer);

        assertFalse(result);
    }

    @Test
    void updateCommitIndex_commitsWhenMajority() {
        logReplicator.matchIndex.put("http://peer1:8080", 5);
        logReplicator.matchIndex.put("http://peer2:8080", 5);
        when(log.getCommitIndex()).thenReturn(3);
        when(log.getLastIndex()).thenReturn(5);
        when(log.getTermAt(5)).thenReturn(2);

        logReplicator.updateCommitIndex();

        verify(log).setCommitIndex(5);
    }

    @Test
    void applyEntries_appliesUpToCommitIndex() {
        when(log.getCommitIndex()).thenReturn(2);
        when(nodeState.getLastApplied()).thenReturn(0);
        LogEntry entry1 = new LogEntry(2, null);
        LogEntry entry2 = new LogEntry(2, null);
        when(log.getEntryAt(1)).thenReturn(entry1);
        when(log.getEntryAt(2)).thenReturn(entry2);

        logReplicator.applyEntries();

        verify(stateMachine).apply(entry1);
        verify(stateMachine).apply(entry2);
        verify(nodeState).setLastApplied(1);
        verify(nodeState).setLastApplied(2);
    }

    @Test
    void applyEntries_handlesExceptionAndExits() {
        when(log.getCommitIndex()).thenReturn(1);
        when(nodeState.getLastApplied()).thenReturn(0);
        when(log.getEntryAt(1)).thenReturn(new LogEntry(2, null));
        doThrow(new RuntimeException()).when(stateMachine).apply(any());

        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkExit(int status) {
                throw new SecurityException("Exit trapped");
            }

            @Override
            public void checkPermission(java.security.Permission perm) {}
        });

        assertThrows(SecurityException.class, () -> logReplicator.applyEntries());

        verify(stateMachine).apply(any());
        System.setSecurityManager(null);
    }

    @Test
    void replicateLoop_stopsWhenNotLeader() {
        String peer = "http://peer1:8080";
        logReplicator.pendingReplication.put(peer, true);
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);

        logReplicator.replicateLoop(peer);

        verify(restTemplate, never()).postForEntity(anyString(), any(), any());
        assertFalse(logReplicator.pendingReplication.get(peer));
    }
}
