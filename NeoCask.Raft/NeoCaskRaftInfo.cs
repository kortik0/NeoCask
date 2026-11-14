using NeoCask.Raft.Enums;

namespace NeoCask.Raft;

public class NeoCaskRaftInfo
{
    /// <summary>
    /// Unique identifier of this node in the cluster. Never changes.
    /// </summary>
    public string NodeId { get; init; }

    /// <summary>
    /// What this node currently believes itself to be.
    /// Changes over time. Always starts as Follower after restart.
    /// </summary>
    public RaftRole Role { get; set; } = RaftRole.Follower;

    /// <summary>
    /// Current term (epoch) this node lives in.
    /// Only increases, never decreases. Used to detect stale messages from old terms.
    /// </summary>
    public ulong CurrentTerm { get; set; } = 0;

    /// <summary>
    /// Candidate this node voted for in CurrentTerm.
    /// Needed to vote at most once per term. Null if no vote yet in this term.
    /// </summary>
    public string VotedFor { get; set; } = string.Empty;

    /// <summary>
    /// Ordered shared log of commands.
    /// LogIndex of an entry is stable and is not the position in the List.
    /// </summary>
    public List<LogEntry> LogEntries { get; set; } = [];

    /// <summary>
    /// Highest log index known to be committed, i.e. stored on a majority of nodes.
    /// Committed does not mean applied to NeoCask yet. Only moves forward.
    /// </summary>
    public long CommitIndex { get; set; } = 0;

    /// <summary>
    /// Highest log index already applied to the local NeoCask.
    /// Always less than or equal to CommitIndex. Only moves forward.
    /// </summary>
    public long LastApplied { get; set; } = 0;

    /// <summary>
    /// Who this node currently considers the leader.
    /// If this node is the leader, contains its own NodeId. Kept only in memory.
    /// </summary>
    public string? LeaderId { get; set; }

    /// <summary>
    /// Maintained only by the leader, empty on other nodes.
    /// For each follower, the index of the next log entry to send to that follower.
    /// </summary>
    public Dictionary<string, long> NextIndex { get; } = new();

    /// <summary>
    /// Maintained only by the leader, empty on other nodes.
    /// For each follower, the highest log index known to be replicated on that follower.
    /// </summary>
    public Dictionary<string, long> MatchIndex { get; } = new();
}