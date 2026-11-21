using NeoCask.Raft.DTO;
using NeoCask.Raft.Enums;
using NeoCask.Raft.Interfaces;

namespace NeoCask.Raft;

public class NeoCaskRaftNode : IDisposable, IAsyncDisposable
{
    private readonly NeoCaskRaftInfo _raftInfo;
    private readonly INeoCaskRaftTransporter _transporter;

    private readonly IReadOnlyList<string> _peers;
    private readonly Lock _lock = new();

    public NeoCaskRaftInfo Info => _raftInfo;

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
            // _electionTimer = new Timer()
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

    public AppendEntriesResp HandleAppendEntries(AppendEntriesReq req)
    {
        lock (_lock)
        {
            if (req.Term < _raftInfo.CurrentTerm)
            {
                return new AppendEntriesResp(_raftInfo.CurrentTerm, false);
            }

            if (req.Term > _raftInfo.CurrentTerm)
            {
                _raftInfo.CurrentTerm = req.Term;
                _raftInfo.Role = RaftRole.Follower;

                _raftInfo.VotedFor = string.Empty;
            }

            if (req.Term == _raftInfo.CurrentTerm && _raftInfo.Role != RaftRole.Follower)
            {
                _raftInfo.Role = RaftRole.Follower;
            }

            _raftInfo.LeaderId = req.LeaderId;

            var logEntry = _raftInfo.LogEntries.Find(log => log.LogIndex == req.PrevLogIndex);

            if ((req.PrevLogIndex > 0 && logEntry is null) ||
                (logEntry is not null && req.PrevLogTerm != logEntry.Term))
            {
                return new AppendEntriesResp(_raftInfo.CurrentTerm, false);
            }

            foreach (var entry in req.Entries)
            {
                var prevEntry = _raftInfo.LogEntries.FindIndex(log => log.LogIndex == entry.LogIndex);
                if (prevEntry == -1)
                {
                    _raftInfo.LogEntries.Add(entry);
                    continue;
                }

                if (entry.Term == _raftInfo.LogEntries[prevEntry].Term) continue;

                var countToEndOfList = _raftInfo.LogEntries.Count - prevEntry;
                _raftInfo.LogEntries.RemoveRange(prevEntry, countToEndOfList);
                _raftInfo.LogEntries.Add(entry);
            }

            var lastNewIndex = req.PrevLogIndex + req.Entries.Count;
            if (req.LeaderCommit > _raftInfo.CommitIndex)
            {
                _raftInfo.CommitIndex = Math.Min(req.LeaderCommit, lastNewIndex);
            }

            return new AppendEntriesResp(_raftInfo.CurrentTerm, true);
        }
    }

    public void DetermineLatestCommit()
    {
        lock (_lock)
        {
            if (_raftInfo.Role != RaftRole.Leader)
            {
                return;
            }

            var startingCommitIndex = _raftInfo.CommitIndex + 1;
            var maxIndex = _raftInfo.LogEntries.LastOrDefault()?.LogIndex ?? 0;

            var currentIndexToCommit = _raftInfo.CommitIndex;

            for (var index = startingCommitIndex; index <= maxIndex; index++)
            {
                var longs = _raftInfo.MatchIndex.Values.Count(indexes => indexes >= index) + 1;
                if (_raftInfo.IsIndexInCurrentTerm(index) && IsMajority(longs))
                {
                    currentIndexToCommit = index;
                }
            }

            _raftInfo.CommitIndex = currentIndexToCommit;
        }
    }

    public async Task ReplicateToPeerAsync(string peer, CancellationToken cancellationToken)
    {
        long prevLogIndex;
        AppendEntriesReq req;

        ulong startedTerm;
        lock (_lock)
        {
            startedTerm = _raftInfo.CurrentTerm;

            if (_raftInfo.Role != RaftRole.Leader)
            {
                return;
            }

            var nextIndex = _raftInfo.NextIndex[peer];
            prevLogIndex = nextIndex - 1;
            var prevLogTerm = (prevLogIndex > 0 && prevLogIndex <= _raftInfo.LogEntries.Count)
                ? _raftInfo.LogEntries[(int)prevLogIndex - 1].Term
                : 0;

            var start = (int)nextIndex - 1;
            var entries = _raftInfo.LogEntries.Slice(start, _raftInfo.LogEntries.Count - start);

            req = new AppendEntriesReq(startedTerm, _raftInfo.NodeId, prevLogIndex, prevLogTerm, entries,
                _raftInfo.CommitIndex);
        }

        AppendEntriesResp entriesResp;
        try
        {
            entriesResp = await _transporter.AppendEntriesAsync(peer, req, cancellationToken);
        }
        catch
        {
            return;
        }

        lock (_lock)
        {
            if (_raftInfo.CurrentTerm != startedTerm ||
                _raftInfo.Role != RaftRole.Leader)
            {
                return;
            }

            if (entriesResp.Term > _raftInfo.CurrentTerm)
            {
                _raftInfo.CurrentTerm = entriesResp.Term;
                _raftInfo.VotedFor = string.Empty;
                _raftInfo.Role = RaftRole.Follower;
                _raftInfo.LeaderId = string.Empty;
                return;
            }

            switch (entriesResp.Success)
            {
                case false:
                {
                    var currentIndex = _raftInfo.NextIndex[peer];
                    var nextIndex = Math.Max(1, currentIndex - 1);
                    _raftInfo.NextIndex[peer] = nextIndex;
                    return;
                }
                case true:
                {
                    var matchIndex = prevLogIndex + req.Entries.Count;
                    _raftInfo.MatchIndex[peer] = Math.Max(_raftInfo.MatchIndex[peer], matchIndex);
                    _raftInfo.NextIndex[peer] = Math.Max(_raftInfo.NextIndex[peer], matchIndex + 1);
                    DetermineLatestCommit();
                    break;
                }
            }
        }
    }

    public async Task ElectionTimeoutAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_raftInfo.Role == RaftRole.Leader)
            {
                return;
            }
        }

        await StartElectionAsync(cancellationToken);

        lock (_lock)
        {
            if (_raftInfo.Role != RaftRole.Leader)
            {
                return;
            }
        }

        await ReplicateToAllPeersAsync(cancellationToken);
    }

    public async Task StartElectionAsync(CancellationToken cancellationToken)
    {
        ulong startedTerm;
        RequestVoteReq? voteRequest = null;

        lock (_lock)
        {
            _raftInfo.Role = RaftRole.Candidate;
            _raftInfo.CurrentTerm += 1;
            startedTerm = _raftInfo.CurrentTerm;
            _raftInfo.VotedFor = _raftInfo.NodeId;
            _raftInfo.LeaderId = "";

            var entry = _raftInfo.LogEntries.LastOrDefault();
            voteRequest = new RequestVoteReq(_raftInfo.CurrentTerm, _raftInfo.NodeId, entry?.LogIndex ?? 0,
                entry?.Term ?? 0);
        }


        var tasks = _peers.Select(async peer =>
        {
            try
            {
                return await _transporter.RequestVoteAsync(peer, voteRequest, cancellationToken);
            }
            catch (Exception e)
            {
                return null;
            }
        });

        var voteResponse = await Task.WhenAll(tasks);

        lock (_lock)
        {
            var isTermChanged = startedTerm != _raftInfo.CurrentTerm;
            var isNotMoreCandidate = _raftInfo.Role != RaftRole.Candidate;

            if (isTermChanged || isNotMoreCandidate)
            {
                return;
            }

            var grantedVotes = 0;
            foreach (var vote in voteResponse)
            {
                if (vote is null)
                {
                    continue;
                }

                if (vote.Term > _raftInfo.CurrentTerm)
                {
                    _raftInfo.Role = RaftRole.Follower;
                    _raftInfo.CurrentTerm = vote.Term;
                    _raftInfo.VotedFor = string.Empty;
                    _raftInfo.LeaderId = string.Empty;
                    return;
                }

                if (vote.IsVoted)
                {
                    grantedVotes += 1;
                }
            }

            //Vote for self
            grantedVotes += 1;

            var isMajorityVoteGrant = grantedVotes > (_peers.Count + 1) / 2;
            if (isMajorityVoteGrant)
            {
                _raftInfo.Role = RaftRole.Leader;
                _raftInfo.VotedFor = _raftInfo.NodeId;

                var lastLogEntry = _raftInfo.LogEntries.LastOrDefault()?.LogIndex ?? 0;

                foreach (var peer in _peers)
                {
                    _raftInfo.NextIndex[peer] = lastLogEntry + 1;
                    _raftInfo.MatchIndex[peer] = 0;
                }

                _raftInfo.LeaderId = _raftInfo.NodeId;
            }
        }
    }

    public async Task ReplicateToAllPeersAsync(CancellationToken cancellationToken)
    {
        var tasks = _peers.Select(async peer =>
        {
            try
            {
                await ReplicateToPeerAsync(peer, cancellationToken);
                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        });

        await Task.WhenAll(tasks);
    }

    private int GetMajorityCount => (_peers.Count + 1) / 2 + 1;

    private bool IsMajority(int count) => GetMajorityCount <= count;

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