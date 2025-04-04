@RestController
@RequiredArgsConstructor
@RequestMapping("/raft/rpc")
public class RaftRpcController {
    private final ElectionManager electionManager;
    private final RaftLogManager raftLogManager;
    private final RaftKVService raftKvService;

    @PostMapping("/requestVote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        return ResponseEntity.ok(electionManager.handleVoteRequest(requestVoteDTO));
    }

    @PostMapping("/appendEntries")
    public ResponseEntity<AppendEntryResponseDTO> appendEntries(@RequestBody AppendEntryDTO dto) {
        return ResponseEntity.ok(raftLogManager.handleAppendEntries(dto));
    }

    @PostMapping("/readIndex")
    public ResponseEntity<ReadIndexResponseDTO> readIndex() {
        return ResponseEntity.ok(raftKvService.getSafeReadIndex());
    }

    @PostMapping("/raft/confirmLeadership")
    public ResponseEntity<HeartbeatResponse> confirmLeadership(){
        return ResponseEntity.ok(raftKvService.handleConfirmLeadership());        
    }
}
