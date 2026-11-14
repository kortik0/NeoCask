using NeoCask.Raft.Interfaces;

namespace NeoCask.Raft;

public class NeoCaskRaftNode
{
    private readonly NeoCaskRaftInfo _raftInfo;
    private readonly INeoCaskRaftTransporter _transporter;

    private IReadOnlyList<string> _nodeIds;
    private readonly object _lock = new();

    public NeoCaskRaftNode(INeoCaskRaftTransporter transporter)
    {
        _transporter = transporter;
    }
}