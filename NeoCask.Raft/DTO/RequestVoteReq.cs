namespace NeoCask.Raft.DTO;

public record RequestVoteReq(
    ulong Term,
    string CandidateId,
    long LastLogIndex,
    ulong LastLogTerm);