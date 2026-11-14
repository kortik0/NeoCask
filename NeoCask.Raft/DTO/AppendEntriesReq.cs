namespace NeoCask.Raft.DTO;

public record AppendEntriesReq(
    ulong Term,
    string LeaderId,
    long PrevLogIndex,
    ulong PrevLogTerm,
    List<LogEntry> Entries,
    long LeaderCommit);