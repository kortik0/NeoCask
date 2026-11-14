namespace NeoCask.Raft.DTO;

public record AppendEntriesResp(ulong Term, bool Success);