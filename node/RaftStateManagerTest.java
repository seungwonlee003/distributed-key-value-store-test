import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class RaftStateManagerTest {

    @Mock
    private HeartbeatManager heartbeatManager;

    @Mock
    private ElectionTimer electionTimer;

    @Mock
    private RaftReplicationManager raftReplicationManager;

    @InjectMocks
    private RaftStateManager raftStateManager;

    private RaftNodeState state;

    @BeforeEach
    public void setUp() {
        state = new RaftNodeState();
    }

    @Test
    public void testBecomeLeader() {
        state.setCurrentRole(Role.FOLLOWER);

        raftStateManager.becomeLeader();

        assertEquals(Role.LEADER, state.getCurrentRole());
        verify(raftReplicationManager).initializeIndices();
        verify(heartbeatManager).startHeartbeats();
    }

    @Test
    public void testBecomeFollower() {
        state.setCurrentTerm(1);
        state.setCurrentRole(Role.LEADER);
        state.setVotedFor("node1");
        state.setCurrentLeader("node1");

        raftStateManager.becomeFollower(2);

        assertEquals(2, state.getCurrentTerm());
        assertEquals(Role.FOLLOWER, state.getCurrentRole());
        assertNull(state.getVotedFor());
        assertNull(state.getCurrentLeader());
        verify(heartbeatManager).stopHeartbeats();
        verify(electionTimer).reset();
    }

    @Test
    public void testResetElectionTimer() {
        raftStateManager.resetElectionTimer();
        verify(electionTimer).reset();
    }
}
