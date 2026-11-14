using NeoCask.Raft.Enums;

namespace NeoCask.Raft;

public record LogEntry(
    long LogIndex,
    ulong Term,
    RaftOperation Operation,
    string Key,
    string? Value
);