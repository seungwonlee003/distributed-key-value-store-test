import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AppendEntriesHandlerTest {
    @Mock
    private RaftLog log;

    @Mock
    private RaftStateManager stateManager;

    @Mock
    private RaftNodeState nodeState;

    @Mock
    private StateMachine stateMachine;

    @Mock
    private ExecutorService applyExecutor;

    @InjectMocks
    private AppendEntriesHandler handler;

    private AppendEntryDTO dto;
    private List<LogEntry> entries;

    @BeforeEach
    void setUp() {
        entries = new ArrayList<>();
        dto = new AppendEntryDTO();
        dto.setTerm(1);
        dto.setLeaderId(2);
        dto.setPrevLogIndex(0);
        dto.setPrevLogTerm(0);
        dto.setEntries(entries);
        dto.setLeaderCommit(0);
        when(nodeState.getCurrentTerm()).thenReturn(1);
    }

    @Test
    void handle_whenLeaderTermLessThanCurrentTerm_returnsFailure() {
        dto.setTerm(0);
        when(nodeState.getCurrentTerm()).thenReturn(1);

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(1, response.getTerm());
        assertFalse(response.isSuccess());
        verify(stateManager, never()).becomeFollower(anyInt());
    }

    @Test
    void handle_whenLeaderTermGreaterThanCurrentTerm_becomesFollower() {
        dto.setTerm(2);
        when(nodeState.getCurrentTerm()).thenReturn(1);

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(2, response.getTerm());
        assertTrue(response.isSuccess());
        verify(stateManager).becomeFollower(2);
        verify(nodeState).setCurrentLeader(2);
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void handle_whenPrevLogMismatch_returnsFailure() {
        dto.setPrevLogIndex(1);
        dto.setPrevLogTerm(1);
        when(log.containsEntryAt(1)).thenReturn(true);
        when(log.getTermAt(1)).thenReturn(2);

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(1, response.getTerm());
        assertFalse(response.isSuccess());
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void handle_withEntriesAndConflict_deletesAndAppends() {
        entries.add(new LogEntry(1, null));
        dto.setEntries(entries);
        dto.setPrevLogIndex(1);
        when(log.containsEntryAt(1)).thenReturn(true);
        when(log.getTermAt(1)).thenReturn(0);
        when(log.containsEntryAt(2)).thenReturn(true);
        when(log.getTermAt(2)).thenReturn(2);

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(1, response.getTerm());
        assertTrue(response.isSuccess());
        verify(log).deleteFrom(2);
        verify(log).appendAll(entries);
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void handle_withEntriesNoConflict_appendsAll() {
        entries.add(new LogEntry(1, null));
        dto.setEntries(entries);
        dto.setPrevLogIndex(1);
        when(log.containsEntryAt(1)).thenReturn(true);
        when(log.getTermAt(1)).thenReturn(0);
        when(log.containsEntryAt(2)).thenReturn(false);

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(1, response.getTerm());
        assertTrue(response.isSuccess());
        verify(log).appendAll(entries);
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void handle_whenLeaderCommitHigher_commitsAndApplies() {
        dto.setLeaderCommit(2);
        dto.setPrevLogIndex(1);
        when(log.getCommitIndex()).thenReturn(0);
        when(nodeState.getLastApplied()).thenReturn(0);
        when(log.containsEntryAt(1)).thenReturn(true);
        when(log.getTermAt(1)).thenReturn(0);
        when(log.getEntryAt(1)).thenReturn(new LogEntry(1, null));
        when(log.getEntryAt(2)).thenReturn(new LogEntry(1, null));

        AppendEntryResponseDTO response = handler.handle(dto);

        assertEquals(1, response.getTerm());
        assertTrue(response.isSuccess());
        verify(log).setCommitIndex(2);
        verify(applyExecutor).submit(any(Runnable.class));
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void applyEntries_appliesUpToCommitIndex() {
        when(log.getCommitIndex()).thenReturn(2);
        when(nodeState.getLastApplied()).thenReturn(0);
        LogEntry entry1 = new LogEntry(1, null);
        LogEntry entry2 = new LogEntry(1, null);
        when(log.getEntryAt(1)).thenReturn(entry1);
        when(log.getEntryAt(2)).thenReturn(entry2);

        // Invoke applyEntries via reflection or mock the executor to run it
        handler.applyEntries();

        verify(stateMachine).apply(entry1);
        verify(stateMachine).apply(entry2);
        verify(nodeState).setLastApplied(1);
        verify(nodeState).setLastApplied(2);
    }

    @Test
    void applyEntries_handlesExceptionAndExits() {
        when(log.getCommitIndex()).thenReturn(1);
        when(nodeState.getLastApplied()).thenReturn(0);
        when(log.getEntryAt(1)).thenReturn(new LogEntry(1, null));
        doThrow(new RuntimeException()).when(stateMachine).apply(any());

        // Redirect System.exit to avoid test termination
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkExit(int status) {
                throw new SecurityException("Exit trapped");
            }

            @Override
            public void checkPermission(java.security.Permission perm) {
            }
        });

        assertThrows(SecurityException.class, () -> handler.applyEntries());

        verify(stateMachine).apply(any());
        System.setSecurityManager(null);
    }
}
