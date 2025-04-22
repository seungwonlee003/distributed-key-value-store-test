import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElectionTimerTest {
    @Mock
    private RaftConfig raftConfig;

    @Mock
    private RaftNode raftNode;

    @Mock
    private ElectionManager electionManager;

    @Mock
    private ScheduledExecutorService scheduler;

    @Mock
    private Random random;

    @Mock
    private ScheduledFuture<?> electionFuture;

    @InjectMocks
    private ElectionTimer electionTimer;

    @BeforeEach
    void setUp() {
        when(raftConfig.getElectionTimeoutMillisMin()).thenReturn(150L);
        when(raftConfig.getElectionTimeoutMillisMax()).thenReturn(300L);
        when(random.nextInt(anyInt())).thenReturn(50); // Controlled random for predictable timeout
    }

    @Test
    void reset_whenNoFuture_schedulesNewElection() {
        when(scheduler.schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(electionFuture);

        electionTimer.reset();

        verify(scheduler).schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS));
        verify(electionFuture, never()).cancel(anyBoolean());
    }

    @Test
    void reset_whenFutureExists_cancelsAndSchedulesNew() {
        when(scheduler.schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(electionFuture);
        electionTimer.reset(); // Set initial future

        reset(scheduler, electionFuture);
        when(scheduler.schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(electionFuture);

        electionTimer.reset();

        verify(electionFuture).cancel(false);
        verify(scheduler).schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void cancel_whenFutureExists_cancelsFuture() {
        when(scheduler.schedule(any(Runnable.class), eq(200L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(electionFuture);
        electionTimer.reset(); // Set future

        electionTimer.cancel();

        verify(electionFuture).cancel(false);
    }

    @Test
    void cancel_whenNoFuture_doesNothing() {
        electionTimer.cancel();

        verify(electionFuture, never()).cancel(anyBoolean());
    }

    @Test
    void reset_schedulesWithRandomTimeoutInRange() {
        when(random.nextInt(151)).thenReturn(100); // 150 + 100 = 250ms
        when(scheduler.schedule(any(Runnable.class), eq(250L), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(electionFuture);

        electionTimer.reset();

        verify(scheduler).schedule(any(Runnable.class), eq(250L), eq(TimeUnit.MILLISECONDS));
    }
}
