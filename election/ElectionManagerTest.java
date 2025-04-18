import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ElectionManagerTest {

    @Mock
    private RaftConfig raftConfig;

    @Mock
    private RaftLog raftLog;

    @Mock
    private RaftNodeState nodeState;

    @Mock
    private RaftStateManager stateManager;

    @Mock
    private RestTemplate restTemplate;

    @InjectMocks
    private ElectionManager electionManager;

    private final int nodeId = 1;
    private final List<String> peerUrls = List.of("http://peer1:8080", "http://peer2:8080");

    @BeforeEach
    void setUp() {
        when(raftConfig.getPeerUrlList()).thenReturn(peerUrls);
        when(raftConfig.getElectionRpcTimeoutMillis()).thenReturn(100L);
        when(nodeState.getNodeId()).thenReturn(nodeId);
    }

    @Test
    void handleVoteRequest_deniesVote_whenRequestTermLessThanCurrentTerm() {
        when(nodeState.getCurrentTerm()).thenReturn(2);
        RequestVoteDTO request = new RequestVoteDTO(1, 2, 0, 0);

        VoteResponseDTO response = electionManager.handleVoteRequest(request);

        assertEquals(2, response.getTerm());
        assertFalse(response.isVoteGranted());
        verify(stateManager, never()).becomeFollower(anyInt());
    }

    @Test
    void handleVoteRequest_grantsVoteAndBecomesFollower_whenRequestTermGreaterThanCurrentTerm() {
        when(nodeState.getCurrentTerm()).thenReturn(1);
        when(nodeState.getVotedFor()).thenReturn(null);
        when(raftLog.getLastTerm()).thenReturn(0);
        when(raftLog.getLastIndex()).thenReturn(0);
        RequestVoteDTO request = new RequestVoteDTO(2, 2, 0, 0);

        VoteResponseDTO response = electionManager.handleVoteRequest(request);

        assertEquals(2, response.getTerm());
        assertTrue(response.isVoteGranted());
        verify(stateManager).becomeFollower(2);
        verify(nodeState).setVotedFor(2);
        verify(stateManager).resetElectionTimer();
    }

    @Test
    void handleVoteRequest_deniesVote_whenAlreadyVotedForDifferentCandidate() {
        when(nodeState.getCurrentTerm()).thenReturn(2);
        when(nodeState.getVotedFor()).thenReturn(3);
        RequestVoteDTO request = new RequestVoteDTO(2, 2, 0, 0);

        VoteResponseDTO response = electionManager.handleVoteRequest(request);

        assertEquals(2, response.getTerm());
        assertFalse(response.isVoteGranted());
        verify(stateManager, never()).resetElectionTimer();
    }

    @Test
    void handleVoteRequest_deniesVote_whenCandidateLogNotUpToDate() {
        when(nodeState.getCurrentTerm()).thenReturn(2);
        when(nodeState.getVotedFor()).thenReturn(null);
        when(raftLog.getLastTerm()).thenReturn(2);
        when(raftLog.getLastIndex()).thenReturn(5);
        RequestVoteDTO request = new RequestVoteDTO(2, 2, 4, 1); // Older term or index

        VoteResponseDTO response = electionManager.handleVoteRequest(request);

        assertEquals(2, response.getTerm());
        assertFalse(response.isVoteGranted());
        verify(stateManager, never()).resetElectionTimer();
    }

    @Test
    void handleVoteRequest_grantsVote_whenAllConditionsMet() {
        when(nodeState.getCurrentTerm()).thenReturn(2);
        when(nodeState.getVotedFor()).thenReturn(null);
        when(raftLog.getLastTerm()).thenReturn(1);
        when(raftLog.getLastIndex()).thenReturn(4);
        RequestVoteDTO request = new RequestVoteDTO(2, 2, 5, 2);

        VoteResponseDTO response = electionManager.handleVoteRequest(request);

        assertEquals(2, response.getTerm());
        assertTrue(response.isVoteGranted());
        verify(nodeState).setVotedFor(2);
        verify(stateManager).resetElectionTimer();
    }

    // Tests for startElection
    @Test
    void startElection_doesNothing_whenAlreadyLeader() {
        when(nodeState.getRole()).thenReturn(Role.LEADER);

        electionManager.startElection();

        verify(nodeState, never()).setCurrentRole(any());
        verify(nodeState, never()).incrementTerm();
    }

    @Test
    void startElection_initiatesElectionAndBecomesLeader_whenMajorityVotes() throws Exception {
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);
        when(nodeState.getCurrentTerm()).thenReturn(1);
        when(raftLog.getLastIndex()).thenReturn(5);
        when(raftLog.getLastTerm()).thenReturn(2);

        VoteResponseDTO voteResponse = new VoteResponseDTO(1, true);
        ResponseEntity<VoteResponseDTO> responseEntity = ResponseEntity.ok(voteResponse);
        when(restTemplate.postForEntity(anyString(), any(), eq(VoteResponseDTO.class)))
                .thenReturn(responseEntity);

        electionManager.startElection();

        verify(nodeState).setCurrentRole(Role.CANDIDATE);
        verify(nodeState).incrementTerm();
        verify(nodeState).setVotedFor(nodeId);
        verify(stateManager).resetElectionTimer();
        verify(stateManager, timeout(1000)).becomeLeader();
    }

    @Test
    void startElection_doesNotBecomeLeader_whenInsufficientVotes() throws Exception {
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);
        when(nodeState.getCurrentTerm()).thenReturn(1);
        when(raftLog.getLastIndex()).thenReturn(5);
        when(raftLog.getLastTerm()).thenReturn(2);

        VoteResponseDTO voteResponse = new VoteResponseDTO(1, false);
        ResponseEntity<VoteResponseDTO> responseEntity = ResponseEntity.ok(voteResponse);
        when(restTemplate.postForEntity(anyString(), any(), eq(VoteResponseDTO.class)))
                .thenReturn(responseEntity);

        electionManager.startElection();

        verify(nodeState).setCurrentRole(Role.CANDIDATE);
        verify(nodeState).incrementTerm();
        verify(nodeState).setVotedFor(nodeId);
        verify(stateManager).resetElectionTimer();
        verify(stateManager, never()).becomeLeader();
    }

    @Test
    void startElection_becomesFollower_whenHigherTermReceived() throws Exception {
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);
        when(nodeState.getCurrentTerm()).thenReturn(1);
        when(raftLog.getLastIndex()).thenReturn(5);
        when(raftLog.getLastTerm()).thenReturn(2);

        VoteResponseDTO voteResponse = new VoteResponseDTO(2, false);
        ResponseEntity<VoteResponseDTO> responseEntity = ResponseEntity.ok(voteResponse);
        when(restTemplate.postForEntity(anyString(), any(), eq(VoteResponseDTO.class)))
                .thenReturn(responseEntity);

        electionManager.startElection();

        verify(nodeState).setCurrentRole(Role.CANDIDATE);
        verify(stateManager).becomeFollower(2);
        verify(stateManager, never()).becomeLeader();
    }

    @Test
    void startElection_handlesTimeoutGracefully() throws Exception {
        when(nodeState.getRole()).thenReturn(Role.FOLLOWER);
        when(nodeState.getCurrentTerm()).thenReturn(1);
        when(raftLog.getLastIndex()).thenReturn(5);
        when(raftLog.getLastTerm()).thenReturn(2);

        when(restTemplate.postForEntity(anyString(), any(), eq(VoteResponseDTO.class)))
                .thenThrow(new RuntimeException("Timeout"));

        electionManager.startElection();

        verify(nodeState).setCurrentRole(Role.CANDIDATE);
        verify(nodeState).incrementTerm();
        verify(nodeState).setVotedFor(nodeId);
        verify(stateManager).resetElectionTimer();
        verify(stateManager, never()).becomeLeader();
    }
}
