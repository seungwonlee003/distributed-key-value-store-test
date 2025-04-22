import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ElectionTimerTest {

    @Mock
    private RaftConfig raftConfig;

    @Mock
    private RaftNode raftNode;

    @Mock
    private ElectionManager electionManager;

    @InjectMocks
    private ElectionTimer electionTimer;

    @BeforeEach
    public void setUp() {
        when(raftConfig.getElectionTimeoutMillisMin()).thenReturn(100L);
        when(raftConfig.getElectionTimeoutMillisMax()).thenReturn(101L);
    }

    @Test
    public void testResetTriggersElection() throws InterruptedException {
        electionTimer.reset();
        Thread.sleep(150);
        verify(electionManager, times(1)).startElection();
    }

    @Test
    public void testCancelPreventsElection() throws InterruptedException {
        electionTimer.reset();
        electionTimer.cancel();
        Thread.sleep(150);
        verify(electionManager, never()).startElection();
    }

    @Test
    public void testShutdownCancelsPendingElection() throws InterruptedException {
        electionTimer.reset();
        electionTimer.shutdown();
        Thread.sleep(150);
        verify(electionManager, never()).startElection();
    }
}
