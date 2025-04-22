import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HeartbeatManagerTest {
    @Mock
    private RaftReplicationManager raftReplicationManager;

    @Mock
    private RaftNodeState raftNodeState;

    @Mock
    private ElectionManager electionManager;

    @Mock
    private ScheduledExecutorService heartbeatExecutor;

    @Mock
    private ScheduledFuture<?> heartbeatFuture;

    @InjectMocks
    private HeartbeatManager heartbeatManager;

    @BeforeEach
    void setUp() {
        // Reset mocks
        reset(raftReplicationManager, raftNodeState, electionManager, heartbeatExecutor, heartbeatFuture);
    }

    @Test
    void startHeartbeats_whenNotRunning_schedulesHeartbeatAndCancelsElectionTimer() {
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(heartbeatFuture);

        heartbeatManager.startHeartbeats();

        verify(electionManager).cancelElectionTimerIfRunning();
        verify(heartbeatExecutor).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void startHeartbeats_whenAlreadyRunning_stopsExistingAndStartsNew() {
        // Simulate existing heartbeat
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(heartbeatFuture);
        heartbeatManager.startHeartbeats(); // First call to set heartbeatFuture

        // Reset mocks to clear interactions
        reset(heartbeatExecutor, heartbeatFuture);
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(heartbeatFuture);

        heartbeatManager.startHeartbeats();

        verify(heartbeatFuture).cancel(false);
        verify(electionManager).cancelElectionTimerIfRunning();
        verify(heartbeatExecutor).scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void startHeartbeats_whenLeader_executesReplication() {
        when(raftNodeState.getRole()).thenReturn(Role.LEADER);
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run(); // Simulate immediate execution
                    return heartbeatFuture;
                });

        heartbeatManager.startHeartbeats();

        verify(raftReplicationManager).start();
        verify(electionManager).cancelElectionTimerIfRunning();
    }

    @Test
    void startHeartbeats_whenNotLeader_doesNotExecuteReplication() {
        when(raftNodeState.getRole()).thenReturn(Role.FOLLOWER);
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(invocation -> {
                    Runnable task = invocation.getArgument(0);
                    task.run(); // Simulate immediate execution
                    return heartbeatFuture;
                });

        heartbeatManager.startHeartbeats();

        verify(raftReplicationManager, never()).start();
        verify(electionManager).cancelElectionTimerIfRunning();
    }

    @Test
    void stopHeartbeats_whenRunning_cancelsHeartbeat() {
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(heartbeatFuture);
        heartbeatManager.startHeartbeats(); // Set heartbeatFuture

        heartbeatManager.stopHeartbeats();

        verify(heartbeatFuture).cancel(false);
    }

    @Test
    void stopHeartbeats_whenNotRunning_doesNothing() {
        heartbeatManager.stopHeartbeats();

        verify(heartbeatFuture, never()).cancel(anyBoolean());
    }

    @Test
    void stopHeartbeats_whenAlreadyDone_doesNotCancelAgain() {
        when(heartbeatExecutor.scheduleAtFixedRate(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(heartbeatFuture);
        when(heartbeatFuture.isDone()).thenReturn(true);
        heartbeatManager.startHeartbeats();

        heartbeatManager.stopHeartbeats();

        verify(heartbeatFuture, never()).cancel(anyBoolean());
    }
}
