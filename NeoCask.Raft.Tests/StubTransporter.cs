using System.Collections.Concurrent;
using NeoCask.Raft.DTO;
using NeoCask.Raft.Enums;
using NeoCask.Raft.Interfaces;
using static NeoCask.Raft.Tests.TestHelpers;


namespace NeoCask.Raft.Tests;

public class StubTransporter : INeoCaskRaftTransporter
{
    public Func<string, RequestVoteReq, CancellationToken, Task<RequestVoteResp>> OnRequestVote { get; set; }
        = (_, _, _) => throw new InvalidOperationException("OnRequestVote is not configured");

    public ConcurrentBag<(string Target, RequestVoteReq Request)> SentVotes { get; } = [];

    public Task<RequestVoteResp> RequestVoteAsync(string targetNodeId, RequestVoteReq requestVoteReq,
        CancellationToken cancellationToken)
    {
        SentVotes.Add((targetNodeId, requestVoteReq));
        return OnRequestVote(targetNodeId, requestVoteReq, cancellationToken);
    }

    public Task<AppendEntriesResp> AppendEntriesAsync(string targetNodeId, AppendEntriesReq appendEntriesReq,
        CancellationToken cancellationToken)
        => throw new NotImplementedException();

    /// Запрос доходит до реального узла напрямую. Узлы из `down` не отвечают (исключение = "не дошло").
    public static Func<string, RequestVoteReq, CancellationToken, Task<RequestVoteResp>> Route(
        IReadOnlyDictionary<string, NeoCaskRaftNode> nodes, params string[] down)
        => (target, req, _) => down.Contains(target)
            ? Task.FromException<RequestVoteResp>(new HttpRequestException($"{target} is down"))
            : Task.FromResult(nodes[target].HandleRequestVote(req));
}

internal static class TestHelpers
{
    public static NeoCaskRaftNode CreateNode(string id, string[] peers, StubTransporter transporter)
        => new(new NeoCaskRaftClusterConfig(id, peers), transporter);

    public static LogEntry Entry(long index, ulong term)
        => new(index, term, RaftOperation.Put, $"k{index}", $"v{index}");

    public static RequestVoteReq Vote(ulong term, string candidate, long lastIndex = 0, ulong lastTerm = 0)
        => new(term, candidate, lastIndex, lastTerm);

    /// Три живых узла, связанных через один транспорт.
    public static (Dictionary<string, NeoCaskRaftNode> Nodes, StubTransporter Transport) CreateCluster()
    {
        var transport = new StubTransporter();
        var nodes = new Dictionary<string, NeoCaskRaftNode>
        {
            ["node1"] = CreateNode("node1", ["node2", "node3"], transport),
            ["node2"] = CreateNode("node2", ["node1", "node3"], transport),
            ["node3"] = CreateNode("node3", ["node1", "node2"], transport),
        };
        transport.OnRequestVote = StubTransporter.Route(nodes);
        return (nodes, transport);
    }
}

public class HandleRequestVoteTests
{
    private static NeoCaskRaftNode NewNode() => CreateNode("node1", ["node2", "node3"], new StubTransporter());

    [Fact]
    public void EmptyNode_NewTerm_GrantsVote()
    {
        var node = NewNode();

        var resp = node.HandleRequestVote(Vote(1, "node2"));

        Assert.True(resp.IsVoted);
        Assert.Equal(1UL, resp.Term);
        Assert.Equal(1UL, node.Info.CurrentTerm);
        Assert.Equal("node2", node.Info.VotedFor);
    }

    [Fact]
    public void SecondCandidateInSameTerm_IsDenied()
    {
        var node = NewNode();
        node.HandleRequestVote(Vote(1, "node2"));

        var resp = node.HandleRequestVote(Vote(1, "node3"));

        Assert.False(resp.IsVoted);
        Assert.Equal("node2", node.Info.VotedFor);
    }

    [Fact]
    public void SameCandidateAskingAgain_GetsVoteAgain()
    {
        var node = NewNode();
        node.HandleRequestVote(Vote(1, "node2"));

        var resp = node.HandleRequestVote(Vote(1, "node2"));

        Assert.True(resp.IsVoted);
    }

    [Fact]
    public void StaleTerm_IsDenied_AndOwnTermUnchanged()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 1;

        var resp = node.HandleRequestVote(Vote(0, "node2"));

        Assert.False(resp.IsVoted);
        Assert.Equal(1UL, resp.Term);
        Assert.Equal(1UL, node.Info.CurrentTerm);
        Assert.Equal("", node.Info.VotedFor);
    }

    [Fact]
    public void HigherTerm_ResetsPreviousVote()
    {
        var node = NewNode();
        node.HandleRequestVote(Vote(1, "node2"));

        var resp = node.HandleRequestVote(Vote(2, "node3"));

        Assert.True(resp.IsVoted);
        Assert.Equal(2UL, node.Info.CurrentTerm);
        Assert.Equal("node3", node.Info.VotedFor);
    }

    [Fact]
    public void HigherTerm_DemotesLeaderToFollower_AndClearsLeaderId()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 1;
        node.Info.Role = RaftRole.Leader;
        node.Info.LeaderId = "node1";

        node.HandleRequestVote(Vote(2, "node2"));

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal("", node.Info.LeaderId);
        Assert.Equal(2UL, node.Info.CurrentTerm);
    }

    [Fact]
    public void CandidateWithOlderLastLogTerm_IsDenied_ButTermIsAdopted()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 2;
        node.Info.LogEntries.Add(Entry(1, 2));

        var resp = node.HandleRequestVote(Vote(3, "node2", lastIndex: 5, lastTerm: 1));

        Assert.False(resp.IsVoted);
        Assert.Equal("", node.Info.VotedFor);
        Assert.Equal(3UL, node.Info.CurrentTerm); // терм обновили, хотя голос не дали
    }

    [Fact]
    public void SameLastLogTerm_ShorterLog_IsDenied()
    {
        var node = NewNode();
        node.Info.LogEntries.AddRange([Entry(1, 1), Entry(2, 1), Entry(3, 1)]);

        var resp = node.HandleRequestVote(Vote(2, "node2", lastIndex: 2, lastTerm: 1));

        Assert.False(resp.IsVoted);
        Assert.Equal("", node.Info.VotedFor);
    }

    [Fact]
    public void SameLastLogTerm_EqualLength_IsGranted()
    {
        var node = NewNode();
        node.Info.LogEntries.AddRange([Entry(1, 1), Entry(2, 1)]);

        var resp = node.HandleRequestVote(Vote(2, "node2", lastIndex: 2, lastTerm: 1));

        Assert.True(resp.IsVoted);
    }

    [Fact]
    public void HigherLastLogTerm_WinsOverLongerLog()
    {
        // кандидат (term 5, index 12) против узла (term 4, index 15)
        var node = NewNode();
        node.Info.CurrentTerm = 5;
        for (var i = 1; i <= 15; i++) node.Info.LogEntries.Add(Entry(i, 4));

        var resp = node.HandleRequestVote(Vote(6, "node2", lastIndex: 12, lastTerm: 5));

        Assert.True(resp.IsVoted);
    }
}

public class StartElectionTests
{
    [Fact]
    public async Task AllPeersAlive_NodeBecomesLeader()
    {
        var (nodes, _) = CreateCluster();
        var node1 = nodes["node1"];

        await node1.StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Leader, node1.Info.Role);
        Assert.Equal(1UL, node1.Info.CurrentTerm);
        Assert.Equal("node1", node1.Info.LeaderId);
        Assert.Equal("node1", node1.Info.VotedFor);

        Assert.Equal(["node2", "node3"], node1.Info.NextIndex.Keys.Order());
        Assert.All(node1.Info.NextIndex.Values, v => Assert.Equal(1L, v));
        Assert.All(node1.Info.MatchIndex.Values, v => Assert.Equal(0L, v));

        Assert.Equal("node1", nodes["node2"].Info.VotedFor);
        Assert.Equal("node1", nodes["node3"].Info.VotedFor);
    }

    [Fact]
    public async Task OnePeerDown_NodeStillBecomesLeader()
    {
        var (nodes, transport) = CreateCluster();
        transport.OnRequestVote = StubTransporter.Route(nodes, down: "node3");

        await nodes["node1"].StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Leader, nodes["node1"].Info.Role);
    }

    [Fact]
    public async Task BothPeersDown_NodeStaysCandidate()
    {
        var (nodes, transport) = CreateCluster();
        transport.OnRequestVote = StubTransporter.Route(nodes, down: ["node2", "node3"]);
        var node1 = nodes["node1"];

        await node1.StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Candidate, node1.Info.Role);
        Assert.Equal(1UL, node1.Info.CurrentTerm);
        Assert.Equal("node1", node1.Info.VotedFor);
        Assert.Equal("", node1.Info.LeaderId);
        Assert.Empty(node1.Info.NextIndex);
    }

    [Fact]
    public async Task PeerWithHigherTerm_NodeBecomesFollower()
    {
        var (nodes, transport) = CreateCluster();
        transport.OnRequestVote = (target, req, _) => Task.FromResult(
            target == "node2"
                ? new RequestVoteResp(7, false)
                : new RequestVoteResp(req.Term, true)); // node3 проголосовал бы, но терм node2 важнее
        var node1 = nodes["node1"];

        await node1.StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Follower, node1.Info.Role);
        Assert.Equal(7UL, node1.Info.CurrentTerm);
        Assert.Equal("", node1.Info.VotedFor);
        Assert.Equal("", node1.Info.LeaderId);
    }

    [Fact]
    public async Task BothPeersHaveNewerLog_NodeStaysCandidate()
    {
        var (nodes, _) = CreateCluster();
        foreach (var id in new[] { "node2", "node3" })
        {
            nodes[id].Info.CurrentTerm = 1;
            nodes[id].Info.LogEntries.Add(Entry(1, 1));
        }

        await nodes["node1"].StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Candidate, nodes["node1"].Info.Role);
        Assert.Equal("", nodes["node1"].Info.LeaderId);
    }

    [Fact]
    public async Task OnePeerHasNewerLog_OtherGrants_NodeBecomesLeader()
    {
        var (nodes, _) = CreateCluster();
        nodes["node2"].Info.CurrentTerm = 1;
        nodes["node2"].Info.LogEntries.Add(Entry(1, 1));

        await nodes["node1"].StartElectionAsync(CancellationToken.None);

        Assert.Equal(RaftRole.Leader, nodes["node1"].Info.Role); // свой голос + node3 = 2 из 3
    }

    [Fact]
    public async Task Election_SendsRequestWithTermAndLastLogOfCandidate()
    {
        var transport = new StubTransporter
        {
            OnRequestVote = (_, req, _) => Task.FromResult(new RequestVoteResp(req.Term, false))
        };
        var node = CreateNode("node1", ["node2", "node3"], transport);
        node.Info.CurrentTerm = 2;
        node.Info.LogEntries.AddRange([Entry(1, 1), Entry(2, 2), Entry(3, 2)]);

        await node.StartElectionAsync(CancellationToken.None);

        Assert.Equal(2, transport.SentVotes.Count);
        Assert.Equal(["node2", "node3"], transport.SentVotes.Select(x => x.Target).Order());
        Assert.All(transport.SentVotes, x =>
        {
            Assert.Equal(3UL, x.Request.Term);
            Assert.Equal("node1", x.Request.CandidateId);
            Assert.Equal(3L, x.Request.LastLogIndex);
            Assert.Equal(2UL, x.Request.LastLogTerm);
        });
    }

    [Fact]
    public async Task EmptyLog_SendsZeroLastLogIndexAndTerm()
    {
        var transport = new StubTransporter
        {
            OnRequestVote = (_, req, _) => Task.FromResult(new RequestVoteResp(req.Term, false))
        };
        var node = CreateNode("node1", ["node2", "node3"], transport);

        await node.StartElectionAsync(CancellationToken.None);

        Assert.All(transport.SentVotes, x =>
        {
            Assert.Equal(0L, x.Request.LastLogIndex);
            Assert.Equal(0UL, x.Request.LastLogTerm);
        });
    }

    [Fact]
    public async Task LateGrantedVotes_AfterTermMovedOn_AreIgnored()
    {
        // Выборы начались в терме 1, пока ждем ответы, приходит запрос с термом 5.
        var pending = new TaskCompletionSource<RequestVoteResp>();
        var transport = new StubTransporter { OnRequestVote = (_, _, _) => pending.Task };
        var node = CreateNode("node1", ["node2", "node3"], transport);

        var election = node.StartElectionAsync(CancellationToken.None);
        Assert.False(election.IsCompleted);

        node.HandleRequestVote(Vote(5, "node2"));
        pending.SetResult(new RequestVoteResp(1, true)); // запоздавшие голоса за терм 1
        await election;

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal(5UL, node.Info.CurrentTerm);
        Assert.Equal("node2", node.Info.VotedFor);
        Assert.Empty(node.Info.NextIndex);
    }

    [Fact]
    public async Task SecondElectionRound_UsesNextTerm()
    {
        var (nodes, transport) = CreateCluster();
        transport.OnRequestVote = StubTransporter.Route(nodes, down: ["node2", "node3"]);
        var node1 = nodes["node1"];

        await node1.StartElectionAsync(CancellationToken.None);
        await node1.StartElectionAsync(CancellationToken.None);

        Assert.Equal(2UL, node1.Info.CurrentTerm);
        Assert.Equal(RaftRole.Candidate, node1.Info.Role);
    }
}

public class HandleAppendEntriesTests
{
    private static NeoCaskRaftNode NewNode() => CreateNode("node1", ["node2", "node3"], new StubTransporter());

    private static NeoCaskRaftNode NewNodeWithLog(params LogEntry[] log)
    {
        var node = NewNode();
        node.Info.LogEntries.AddRange(log);
        return node;
    }

    /// Запрос от node2 с записями. LeaderCommit по умолчанию 0, меняется через `with`.
    private static AppendEntriesReq Append(ulong term, long prevIndex, ulong prevTerm, params LogEntry[] entries)
        => new(term, "node2", prevIndex, prevTerm, entries.ToList(), 0);

    // ---------- Терм и роли ----------

    [Fact]
    public void StaleTerm_IsRejected_AndStateUnchanged()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 2;

        var resp = node.HandleAppendEntries(Append(1, 0, 0));

        Assert.False(resp.Success);
        Assert.Equal(2UL, resp.Term);
        Assert.Equal(2UL, node.Info.CurrentTerm);
        Assert.Equal("", node.Info.LeaderId);
    }

    [Fact]
    public void Heartbeat_OnEmptyLog_Succeeds_AndSetsLeader()
    {
        var node = NewNode();

        var resp = node.HandleAppendEntries(Append(1, 0, 0));

        Assert.True(resp.Success);
        Assert.Equal(1UL, resp.Term);
        Assert.Equal(1UL, node.Info.CurrentTerm);
        Assert.Equal("node2", node.Info.LeaderId);
    }

    [Fact]
    public void HigherTerm_AdoptsTerm_ResetsVote_BecomesFollower()
    {
        var node = NewNode();
        node.HandleRequestVote(Vote(1, "node3"));
        node.Info.Role = RaftRole.Candidate;

        node.HandleAppendEntries(Append(3, 0, 0));

        Assert.Equal(3UL, node.Info.CurrentTerm);
        Assert.Equal("", node.Info.VotedFor);
        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal("node2", node.Info.LeaderId);
    }

    [Fact]
    public void HigherTerm_DemotesLeaderToFollower()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 1;
        node.Info.Role = RaftRole.Leader;
        node.Info.LeaderId = "node1";

        node.HandleAppendEntries(Append(2, 0, 0));

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal("node2", node.Info.LeaderId);
    }

    [Fact]
    public void SameTerm_Candidate_BecomesFollower_KeepsOwnVote()
    {
        var node = NewNode();
        node.Info.CurrentTerm = 1;
        node.Info.Role = RaftRole.Candidate;
        node.Info.VotedFor = "node1";

        node.HandleAppendEntries(Append(1, 0, 0));

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal("node1", node.Info.VotedFor); // в этом терме мы уже проголосовали, голос не сбрасывается
        Assert.Equal("node2", node.Info.LeaderId);
    }

    // ---------- Проверка PrevLogIndex / PrevLogTerm ----------

    [Fact]
    public void PrevLogIndex_BeyondOurLog_IsRejected()
    {
        var node = NewNodeWithLog(Entry(1, 1));

        var resp = node.HandleAppendEntries(Append(1, 3, 1, Entry(4, 1)));

        Assert.False(resp.Success);
        Assert.Single(node.Info.LogEntries);
    }

    [Fact]
    public void PrevLogIndex_OnEmptyLog_IsRejected()
    {
        var node = NewNode();

        var resp = node.HandleAppendEntries(Append(1, 1, 1, Entry(2, 1)));

        Assert.False(resp.Success);
        Assert.Empty(node.Info.LogEntries);
    }

    [Fact]
    public void PrevLogTerm_Mismatch_IsRejected()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1));

        var resp = node.HandleAppendEntries(Append(2, 2, 2, Entry(3, 2)));

        Assert.False(resp.Success);
        Assert.Equal(2, node.Info.LogEntries.Count);
    }

    [Fact]
    public void PrevLog_Matches_Succeeds()
    {
        var node = NewNodeWithLog(Entry(1, 1));

        var resp = node.HandleAppendEntries(Append(1, 1, 1));

        Assert.True(resp.Success);
    }

    [Fact]
    public void RejectedByLogMismatch_StillAdoptsTermAndLeader()
    {
        // Лидер терма легитимен, даже если журналы пока не совпали
        var node = NewNodeWithLog(Entry(1, 1));

        var resp = node.HandleAppendEntries(Append(2, 3, 1));

        Assert.False(resp.Success);
        Assert.Equal(2UL, resp.Term);
        Assert.Equal(2UL, node.Info.CurrentTerm);
        Assert.Equal("node2", node.Info.LeaderId);
    }

    // ---------- Дописывание записей (Часть 3) ----------

    [Fact]
    public void NewEntries_AreAppendedToEmptyLog()
    {
        var node = NewNode();

        var resp = node.HandleAppendEntries(Append(1, 0, 0, Entry(1, 1), Entry(2, 1)));

        Assert.True(resp.Success);
        Assert.Equal([1L, 2L], node.Info.LogEntries.Select(e => e.LogIndex));
    }

    [Fact]
    public void NewEntries_AreAppendedAfterExisting()
    {
        var node = NewNodeWithLog(Entry(1, 1));

        var resp = node.HandleAppendEntries(Append(1, 1, 1, Entry(2, 1), Entry(3, 1)));

        Assert.True(resp.Success);
        Assert.Equal([1L, 2L, 3L], node.Info.LogEntries.Select(e => e.LogIndex));
    }

    [Fact]
    public void SameRequestTwice_DoesNotDuplicateEntries()
    {
        var node = NewNode();
        var req = Append(1, 0, 0, Entry(1, 1), Entry(2, 1));

        node.HandleAppendEntries(req);
        var resp = node.HandleAppendEntries(req);

        Assert.True(resp.Success);
        Assert.Equal([1L, 2L], node.Info.LogEntries.Select(e => e.LogIndex));
    }

    [Fact]
    public void PartiallyOverlappingRequest_AppendsOnlyMissingEntries()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1));

        var resp = node.HandleAppendEntries(Append(1, 0, 0, Entry(1, 1), Entry(2, 1), Entry(3, 1)));

        Assert.True(resp.Success);
        Assert.Equal([1L, 2L, 3L], node.Info.LogEntries.Select(e => e.LogIndex));
    }

    [Fact]
    public void ConflictingSuffix_IsTruncatedAndReplaced()
    {
        // 2, 3 и 4 записаны старым лидером в терме 1, новый лидер (терм 2) перезаписывает с индекса 2
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1), Entry(4, 1));

        var resp = node.HandleAppendEntries(Append(2, 1, 1, Entry(2, 2)));

        Assert.True(resp.Success);
        Assert.Equal([1L, 2L], node.Info.LogEntries.Select(e => e.LogIndex));
        Assert.Equal([1UL, 2UL], node.Info.LogEntries.Select(e => e.Term));
    }

    [Fact]
    public void ConflictInTheMiddleOfBatch_KeepsMatchingPrefix()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1));

        var resp = node.HandleAppendEntries(Append(2, 1, 1, Entry(2, 1), Entry(3, 2)));

        Assert.True(resp.Success);
        Assert.Equal([1UL, 1UL, 2UL], node.Info.LogEntries.Select(e => e.Term));
    }

    // Ловушка 1: запоздалые запросы не должны стирать хорошие записи

    [Fact]
    public void StaleDuplicateRequest_DoesNotTruncateLog()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1));

        var resp = node.HandleAppendEntries(Append(1, 1, 1, Entry(2, 1)));

        Assert.True(resp.Success);
        Assert.Equal(3, node.Info.LogEntries.Count);
    }

    [Fact]
    public void Heartbeat_DoesNotTruncateLog()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1));

        var resp = node.HandleAppendEntries(Append(1, 1, 1));

        Assert.True(resp.Success);
        Assert.Equal(3, node.Info.LogEntries.Count);
    }

    // ---------- CommitIndex (Часть 3) ----------

    [Theory]
    [InlineData(0, 0)]
    [InlineData(2, 2)] // лидер закоммитил меньше, чем мы получили
    [InlineData(3, 3)]
    [InlineData(10, 3)] // лидер закоммитил больше, но мы знаем только до 3
    public void CommitIndex_IsLimitedByLastEntryFromRequest(long leaderCommit, long expected)
    {
        var node = NewNode();
        var req = Append(1, 0, 0, Entry(1, 1), Entry(2, 1), Entry(3, 1)) with { LeaderCommit = leaderCommit };

        node.HandleAppendEntries(req);

        Assert.Equal(expected, node.Info.CommitIndex);
    }

    // Ловушка 2: нельзя коммитить хвост, который лидер не проверял

    [Fact]
    public void Heartbeat_DoesNotCommitUnverifiedTail()
    {
        // у нас 1-5, лидер подтвердил совпадение только до 2
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1), Entry(4, 1), Entry(5, 1));
        var req = Append(1, 2, 1) with { LeaderCommit = 5 };

        node.HandleAppendEntries(req);

        Assert.Equal(2L, node.Info.CommitIndex);
    }

    [Fact]
    public void CommitIndex_NeverDecreases()
    {
        var node = NewNodeWithLog(Entry(1, 1), Entry(2, 1), Entry(3, 1));
        node.Info.CommitIndex = 3;
        var req = Append(1, 3, 1) with { LeaderCommit = 1 };

        node.HandleAppendEntries(req);

        Assert.Equal(3L, node.Info.CommitIndex);
    }

    [Fact]
    public void RejectedRequest_DoesNotChangeLogOrCommitIndex()
    {
        var node = NewNodeWithLog(Entry(1, 1));
        var req = Append(1, 3, 1, Entry(4, 1)) with { LeaderCommit = 5 };

        var resp = node.HandleAppendEntries(req);

        Assert.False(resp.Success);
        Assert.Equal(0L, node.Info.CommitIndex);
        Assert.Single(node.Info.LogEntries);
    }
}