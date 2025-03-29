@RestController
@RequiredArgsConstructor
@RequestMapping("/raft")
public class RaftRpcController {
    private final ElectionManager electionManager;
    private final RaftLogManager raftLogManager;

    @PostMapping("/requestVote")
    public ResponseEntity<VoteResponseDTO> vote(@RequestBody RequestVoteDTO requestVoteDTO) {
        return ResponseEntity.ok(electionManager.handleVoteRequest(requestVoteDTO));
    }

    @PostMapping("/appendEntries")
    public ResponseEntity<AppendEntryResponseDTO> appendEntries(@RequestBody AppendEntryDTO dto) {
        return ResponseEntity.ok(raftLogManager.handleAppendEntries(dto));
    }
}
