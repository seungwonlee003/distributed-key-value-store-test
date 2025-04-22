import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClientRequestHandlerTest {
    @Mock
    private RaftLog raftLog;

    @Mock
    private RaftNodeState raftNodeState;

    @Mock
    private RaftConfig raftConfig;

    @InjectMocks
    private ClientRequestHandler handler;

    private LogEntry clientEntry;

    @BeforeEach
    void setUp() {
        clientEntry = mock(LogEntry.class);
        when(raftConfig.getClientRequestTimeoutMillis()).thenReturn(1000L);
        when(raftLog.getLastIndex()).thenReturn(1);
    }

    @Test
    void handle_whenLeaderAndEntryApplied_returnsTrue() {
        when(raftNodeState.getRole()).thenReturn(Role.LEADER);
        when(raftNodeState.getLastApplied()).thenReturn(1);

        boolean result = handler.handle(clientEntry);

        verify(raftLog).append(clientEntry);
        assertTrue(result);
    }

    @Test
    void handle_whenLeaderAndTimeout_returnsFalse() {
        when(raftNodeState.getRole()).thenReturn(Role.LEADER);
        when(raftNodeState.getLastApplied()).thenReturn(0);

        boolean result = handler.handle(clientEntry);

        verify(raftLog).append(clientEntry);
        assertFalse(result);
    }

    @Test
    void handle_whenNotLeader_returnsFalse() {
        when(raftNodeState.getRole()).thenReturn(Role.FOLLOWER);

        boolean result = handler.handle(clientEntry);

        verify(raftLog).append(clientEntry);
        assertFalse(result);
    }

    @Test
    void handle_whenInterrupted_returnsFalse() {
        when(raftNodeState.getRole()).thenReturn(Role.LEADER);
        when(raftNodeState.getLastApplied()).thenReturn(0);
        // Simulate interruption by mocking Thread.sleep to throw InterruptedException
        try {
            doThrow(InterruptedException.class).when(raftNodeState).getLastApplied();
        } catch (InterruptedException e) {
            // Expected in test setup
        }

        boolean result = handler.handle(clientEntry);

        verify(raftLog).append(clientEntry);
        assertFalse(result);
        assertTrue(Thread.currentThread().isInterrupted());
    }
}
