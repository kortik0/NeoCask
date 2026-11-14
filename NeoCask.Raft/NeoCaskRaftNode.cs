using NeoCask.Raft.DTO;
using NeoCask.Raft.Enums;
using NeoCask.Raft.Interfaces;

namespace NeoCask.Raft;

public class NeoCaskRaftNode : IDisposable, IAsyncDisposable
{
    private readonly NeoCaskRaftInfo _raftInfo;
    private readonly INeoCaskRaftTransporter _transporter;

    private readonly IReadOnlyList<string> _peers;
    private readonly object _lock = new();

    private Timer? _heartbeatTimer;
    private Timer? _electionTimer;

    private CancellationTokenSource _cancellationTokenSource;

    public NeoCaskRaftNode(NeoCaskRaftClusterConfig clusterConfig, INeoCaskRaftTransporter transporter)
    {
        _raftInfo = new NeoCaskRaftInfo()
        {
            NodeId = clusterConfig.NodeId,
        };
        _peers = [.. clusterConfig.PeersIds];
        _transporter = transporter;
    }

    public void Start()
    {
        lock (_lock)
        {
            _cancellationTokenSource = new CancellationTokenSource();
            _electionTimer = new Timer()
        }
    }

    // TODO: Should be simplified later
    public RequestVoteResp HandleRequestVote(RequestVoteReq requestVoteReq)
    {
        RequestVoteResp resp;
        lock (_lock)
        {
            if (requestVoteReq.Term < _raftInfo.CurrentTerm)
            {
                resp = new RequestVoteResp(_raftInfo.CurrentTerm, false);
                return resp;
            }

            if (requestVoteReq.Term > _raftInfo.CurrentTerm)
            {
                _raftInfo.CurrentTerm = requestVoteReq.Term;
                _raftInfo.Role = RaftRole.Follower;

                _raftInfo.VotedFor = "";
                _raftInfo.LeaderId = "";
            }

            if (!string.IsNullOrWhiteSpace(_raftInfo.VotedFor) && requestVoteReq.CandidateId != _raftInfo.VotedFor)
            {
                resp = new RequestVoteResp(_raftInfo.CurrentTerm, false);
                return resp;
            }

            var lastLogEntry = _raftInfo.LogEntries.LastOrDefault();
            if (lastLogEntry is null)
            {
                _raftInfo.VotedFor = requestVoteReq.CandidateId;
                resp = new RequestVoteResp(_raftInfo.CurrentTerm, true);
                return resp;
            }

            var lastTermInCurrentLogEntries = lastLogEntry.Term;
            if (requestVoteReq.LastLogTerm < lastTermInCurrentLogEntries)
            {
                resp = new RequestVoteResp(_raftInfo.CurrentTerm, false);
                return resp;
            }

            var lastIndexInCurrentLogEntries = lastLogEntry.LogIndex;
            if (requestVoteReq.LastLogTerm == lastTermInCurrentLogEntries)
            {
                if (requestVoteReq.LastLogIndex > lastIndexInCurrentLogEntries ||
                    requestVoteReq.LastLogIndex == lastIndexInCurrentLogEntries)
                {
                    _raftInfo.VotedFor = requestVoteReq.CandidateId;
                    resp = new RequestVoteResp(_raftInfo.CurrentTerm, true);
                    return resp;
                }

                if (requestVoteReq.LastLogIndex < lastIndexInCurrentLogEntries)
                {
                    resp = new RequestVoteResp(_raftInfo.CurrentTerm, false);
                    return resp;
                }
            }

            _raftInfo.VotedFor = requestVoteReq.CandidateId;
            resp = new RequestVoteResp(_raftInfo.CurrentTerm, true);
            return resp;
        }
    }

    public void Dispose()
    {
        _heartbeatTimer?.Dispose();
        _electionTimer?.Dispose();
        _cancellationTokenSource?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (_heartbeatTimer != null) await _heartbeatTimer.DisposeAsync();
        if (_electionTimer != null) await _electionTimer.DisposeAsync();
    }
}

public class NeoCaskTimerConfiguration
{
    public int MinElectionTime { get; init; } = 300;
    public int MaxElectionTime { get; init; } = 700;

    public int LeaderHeartbeatTime { get; init; } = 100;
}