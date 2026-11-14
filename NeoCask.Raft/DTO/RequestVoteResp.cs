namespace NeoCask.Raft.DTO;

public record RequestVoteResp(
    ulong Term,
    bool IsVoted
);