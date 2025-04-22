import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RaftStateManagerTest {
    @Mock
    private RaftNodeState state;

    @Mock
    private HeartbeatManager heartbeatManager;

    @Mock
    private ElectionTimer electionTimer;

    @Mock
    private RaftReplicationManager raftReplicationManager;

    @InjectMocks
    private RaftStateManager raftStateManager;

    @BeforeEach
    void setUp() {
        when(state.getNodeId()).thenReturn(1);
        when(state.getCurrentTerm()).thenReturn(2);
    }

    @Test
    void becomeLeader_setsRoleAndInitializes() {
        raftStateManager.becomeLeader();

        verify(state).setCurrentRole(Role.LEADER);
        verify(raftReplicationManager).initializeIndices();
        verify(heartbeatManager).startHeartbeats();
    }

    @Test
    void becomeFollower_updatesStateAndResetsTimer() {
        raftStateManager.becomeFollower(3);

        verify(state).setCurrentTerm(3);
        verify(state).setCurrentRole(Role.FOLLOWER);
        verify(state).setVotedFor(null);
        verify(state).setCurrentLeader(null);
        verify(heartbeatManager).stopHeartbeats();
        verify(electionTimer).reset();
    }

    @Test
    void resetElectionTimer_delegatesToElectionTimer() {
        raftStateManager.resetElectionTimer();

        verify(electionTimer).reset();
    }
}
