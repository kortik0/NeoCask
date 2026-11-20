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

    public Func<string, AppendEntriesReq, CancellationToken, Task<AppendEntriesResp>> OnAppendEntries { get; set; }
        = (_, _, _) => throw new InvalidOperationException("OnAppendEntries is not configured");

    public ConcurrentBag<(string Target, AppendEntriesReq Request)> SentAppends { get; } = [];

    public Task<AppendEntriesResp> AppendEntriesAsync(string targetNodeId, AppendEntriesReq appendEntriesReq,
        CancellationToken cancellationToken)
    {
        SentAppends.Add((targetNodeId, appendEntriesReq));
        return OnAppendEntries(targetNodeId, appendEntriesReq, cancellationToken);
    }

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

public class RaftHelpersTests
{
    private static StubTransporter Responding(Func<AppendEntriesReq, AppendEntriesResp> respond)
        => new() { OnAppendEntries = (_, req, _) => Task.FromResult(respond(req)) };


    private static StubTransporter AlwaysSuccess()
        => Responding(req => new AppendEntriesResp(req.Term, true));


    private static NeoCaskRaftNode LeaderNode(StubTransporter transport, ulong term, params LogEntry[] log)
    {
        var node = CreateNode("node1", ["node2", "node3"], transport);
        node.Info.CurrentTerm = term;
        node.Info.Role = RaftRole.Leader;
        node.Info.LeaderId = "node1";
        node.Info.LogEntries.AddRange(log);

        var next = (log.LastOrDefault()?.LogIndex ?? 0) + 1;
        foreach (var peer in new[] { "node2", "node3" })
        {
            node.Info.NextIndex[peer] = next;
            node.Info.MatchIndex[peer] = 0;
        }

        return node;
    }


    [Fact]
    public void FindLogEntry_EmptyLog_ReturnsNull()
    {
        var node = LeaderNode(AlwaysSuccess(), 1);

        var entry = node.Info.FindLogEntry(1);

        Assert.Null(entry);
    }

    [Fact]
    public void FindLogEntry_ZeroOrNegativeIndex_ReturnsNull()
    {
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1));

        Assert.Null(node.Info.FindLogEntry(0));
        Assert.Null(node.Info.FindLogEntry(-5));
    }

    [Fact]
    public void FindLogEntry_IndexAboveLast_ReturnsNull()
    {
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1));

        var entry = node.Info.FindLogEntry(3);

        Assert.Null(entry);
    }

    [Fact]
    public void FindLogEntry_ExistingEntries_ReturnsCorrectEntry()
    {
        var node = LeaderNode(AlwaysSuccess(), 2,
            Entry(1, 1), Entry(2, 1), Entry(3, 2));

        var first = node.Info.FindLogEntry(1);
        var middle = node.Info.FindLogEntry(2);
        var last = node.Info.FindLogEntry(3);

        Assert.NotNull(first);
        Assert.Equal(1L, first.LogIndex);
        Assert.Equal(1UL, first.Term);

        Assert.NotNull(middle);
        Assert.Equal(2L, middle.LogIndex);
        Assert.Equal(1UL, middle.Term);

        Assert.NotNull(last);
        Assert.Equal(3L, last.LogIndex);
        Assert.Equal(2UL, last.Term);
    }

    [Fact]
    public void FindLogEntry_AfterLogCompaction_ResolvesOffsetCorrectly()
    {
        // Имитируем усечение лога (снэпшот до 10-го индекса), лог начинается с 11
        var node = LeaderNode(AlwaysSuccess(), 3,
            Entry(11, 2), Entry(12, 3), Entry(13, 3));

        var entry = node.Info.FindLogEntry(12);

        Assert.NotNull(entry);
        Assert.Equal(12L, entry.LogIndex);
        Assert.Equal(3UL, entry.Term);
    }

    [Fact]
    public void FindLogEntry_AfterLogCompaction_IndexBeforeFirst_ReturnsNull()
    {
        // Записи с 1 по 10 уже удалены снэпшотом
        var node = LeaderNode(AlwaysSuccess(), 3,
            Entry(11, 2), Entry(12, 3));

        Assert.Null(node.Info.FindLogEntry(10));
        Assert.Null(node.Info.FindLogEntry(1));
    }

    [Fact]
    public void IsIndexInCurrentTerm_EmptyLog_ReturnsFalse()
    {
        var node = LeaderNode(AlwaysSuccess(), 1);

        Assert.False(node.Info.IsIndexInCurrentTerm(1));
    }

    [Fact]
    public void IsIndexInCurrentTerm_IndexAboveLast_ReturnsFalse()
    {
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1));

        Assert.False(node.Info.IsIndexInCurrentTerm(3));
    }

    [Fact]
    public void IsIndexInCurrentTerm_EntryFromCurrentTerm_ReturnsTrue()
    {
        var node = LeaderNode(AlwaysSuccess(), 2, Entry(1, 1), Entry(2, 2));

        Assert.True(node.Info.IsIndexInCurrentTerm(2));
    }

    [Fact]
    public void IsIndexInCurrentTerm_EntryFromOldTerm_ReturnsFalse()
    {
        var node = LeaderNode(AlwaysSuccess(), 2, Entry(1, 1), Entry(2, 2));

        Assert.False(node.Info.IsIndexInCurrentTerm(1));
    }
}

public class DetermineLatestCommitTests
{
    // Индексы от from до to включительно. Нужен, чтобы текущий терм шёл подряд до конца журнала.
    private static long[] Indexes(long from, long to)
        => Enumerable.Range((int)from, (int)(to - from + 1)).Select(i => (long)i).ToArray();

    // Журнал 1..lastIndex. У индексов из currentTermIndexes терм currentTerm, у остальных previousTerm.
    private static void AddLogEntries(
        NeoCaskRaftNode node,
        long lastIndex,
        ulong previousTerm,
        ulong currentTerm,
        params long[] currentTermIndexes)
    {
        var currentTermSet = currentTermIndexes.ToHashSet();

        for (var index = 1L; index <= lastIndex; index++)
        {
            var term = currentTermSet.Contains(index) ? currentTerm : previousTerm;
            node.Info.LogEntries.Add(Entry(index, term));
        }
    }

    [Fact]
    public void ShouldCommitHighestCurrentTermIndexReplicatedOnMajority()
    {
        // 5 узлов, большинство 3. Индекс 9 у лидера и ещё двух, индекс 10 только у двух.
        var node = CreateNode(3, 0, 12, 10, 9, 7, 3);
        AddLogEntries(node, 12, 2, 3, Indexes(9, 12));

        node.DetermineLatestCommit();

        Assert.Equal(9, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldNotCommitEntryFromPreviousTermWithoutCurrentTermEntry()
    {
        // Индексы до 9 на большинстве, но терм старый. Запись текущего терма (10) только у двух узлов.
        var node = CreateNode(3, 2, 12, 10, 9, 7, 3);
        AddLogEntries(node, 12, 2, 3, Indexes(10, 12));

        node.DetermineLatestCommit();

        Assert.Equal(2, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldCommitPreviousTermEntriesTogetherWithCurrentTermEntry()
    {
        // Записи 8 и 9 старого терма сами не коммитятся. Запись 10 текущего терма лежит на трёх узлах,
        // и вместе с ней коммитится всё, что перед ней.
        var node = CreateNode(3, 0, 12, 10, 10, 7, 3);
        AddLogEntries(node, 12, 2, 3, Indexes(10, 12));

        node.DetermineLatestCommit();

        Assert.Equal(10, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldNotCommitCurrentTermEntryReplicatedOnlyOnMinority()
    {
        // Индекс 4 (текущий терм) у двух узлов. Индекс 3 у трёх, но терм старый.
        var node = CreateNode(3, 2, 12, 10, 3, 2, 1);
        AddLogEntries(node, 12, 2, 3, Indexes(4, 12));

        node.DetermineLatestCommit();

        Assert.Equal(2, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldUseCorrectMajorityForEvenNumberOfNodes()
    {
        // 4 узла, большинство 3. Индекс 20 у лидера и ещё двух, индекс 21 только у двух.
        var node = CreateNode(3, 0, 100, 50, 20, 10);
        AddLogEntries(node, 100, 2, 3, Indexes(20, 100));

        node.DetermineLatestCommit();

        Assert.Equal(20, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldCommitEntryOnSingleNodeCluster()
    {
        var node = CreateNode(3, 0, 5);
        AddLogEntries(node, 5, 2, 3, Indexes(5, 5));

        node.DetermineLatestCommit();

        Assert.Equal(5, node.Info.CommitIndex);
    }

    [Fact]
    public void EmptyLog_DoesNotThrow_AndCommitsNothing()
    {
        var node = CreateNode(3, 0, 0, 0, 0); // журнал не заполняем

        node.DetermineLatestCommit();

        Assert.Equal(0, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldCountLeaderItself()
    {
        // 3 узла, большинство 2. Лидер и node-1 имеют индекс 5, node-2 отстаёт.
        // Если лидера не считать, подтверждений будет 1, и коммит останется 0.
        var node = CreateNode(3, 0, 5, 5, 3);
        AddLogEntries(node, 5, 2, 3, Indexes(1, 5));

        node.DetermineLatestCommit();

        Assert.Equal(5, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldNotCommitWhenOnlyLeaderHasEntry()
    {
        // 3 узла, пиры ничего не подтвердили. Один лидер это 1 из 3.
        var node = CreateNode(3, 0, 5, 0, 0);
        AddLogEntries(node, 5, 2, 3, Indexes(1, 5));

        node.DetermineLatestCommit();

        Assert.Equal(0, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldAdvanceCommitIndexFromExistingValue()
    {
        var node = CreateNode(3, 4, 12, 10, 9, 7, 3);
        AddLogEntries(node, 12, 2, 3, Indexes(9, 12));

        node.DetermineLatestCommit();

        Assert.Equal(9, node.Info.CommitIndex);
    }

    [Fact]
    public void ShouldNeverDecreaseCommitIndex()
    {
        // Пиры «откатились» ниже уже закоммиченного. CommitIndex должен остаться 10.
        var node = CreateNode(3, 10, 12, 5, 4, 3, 2);
        AddLogEntries(node, 12, 2, 3, Indexes(1, 12));

        node.DetermineLatestCommit();

        Assert.Equal(10, node.Info.CommitIndex);
    }

    [Fact]
    public void NonLeader_DoesNotCommit()
    {
        var node = CreateNode(3, 0, 12, 10, 9, 7, 3);
        AddLogEntries(node, 12, 2, 3, Indexes(1, 12));
        node.Info.Role = RaftRole.Follower;

        node.DetermineLatestCommit();

        Assert.Equal(0, node.Info.CommitIndex);
    }

    // matchIndexes[0] это последний индекс самого лидера. В MatchIndex он не попадает, как и в боевом коде.
    // Остальные значения это пиры.
    private static NeoCaskRaftNode CreateNode(
        ulong currentTerm,
        long commitIndex,
        params long[] matchIndexes)
    {
        var nodeIds = Enumerable.Range(0, matchIndexes.Length)
            .Select(index => $"node-{index}")
            .ToArray();

        var clusterConfig = new NeoCaskRaftClusterConfig(
            nodeIds[0],
            [.. nodeIds.Skip(1)]);

        var node = new NeoCaskRaftNode(clusterConfig, null!);

        node.Info.Role = RaftRole.Leader;
        node.Info.CurrentTerm = currentTerm;
        node.Info.CommitIndex = commitIndex;

        for (var i = 1; i < nodeIds.Length; i++)
        {
            node.Info.MatchIndex.Add(nodeIds[i], matchIndexes[i]);
        }

        return node;
    }

    // AddLogEntries без изменений, остаётся твой.
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

public class ReplicateToPeerTests
{
    private const string Peer = "node2";

    /// Лидер node1 в терме `term`. NextIndex пиров = последний индекс + 1, MatchIndex = 0.
    private static NeoCaskRaftNode LeaderNode(StubTransporter transport, ulong term, params LogEntry[] log)
    {
        var node = CreateNode("node1", ["node2", "node3"], transport);
        node.Info.CurrentTerm = term;
        node.Info.Role = RaftRole.Leader;
        node.Info.LeaderId = "node1";
        node.Info.LogEntries.AddRange(log);

        var next = (log.LastOrDefault()?.LogIndex ?? 0) + 1;
        foreach (var peer in new[] { "node2", "node3" })
        {
            node.Info.NextIndex[peer] = next;
            node.Info.MatchIndex[peer] = 0;
        }

        return node;
    }

    private static StubTransporter Responding(Func<AppendEntriesReq, AppendEntriesResp> respond)
        => new() { OnAppendEntries = (_, req, _) => Task.FromResult(respond(req)) };

    private static StubTransporter AlwaysSuccess()
        => Responding(req => new AppendEntriesResp(req.Term, true));

    private static StubTransporter AlwaysReject()
        => Responding(req => new AppendEntriesResp(req.Term, false));

    // ---------- Сборка запроса ----------

    [Fact]
    public async Task NotLeader_SendsNothing()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 1);
        node.Info.Role = RaftRole.Follower;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Empty(transport.SentAppends);
    }

    [Fact]
    public async Task EmptyLog_SendsHeartbeatWithZeroPrev()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 3);

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        var (target, req) = Assert.Single(transport.SentAppends);
        Assert.Equal(Peer, target);
        Assert.Equal(3UL, req.Term);
        Assert.Equal("node1", req.LeaderId);
        Assert.Equal(0L, req.PrevLogIndex);
        Assert.Equal(0UL, req.PrevLogTerm);
        Assert.Empty(req.Entries);
    }

    [Fact]
    public async Task NextIndexInMiddle_SendsPrevAndTailEntries()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 2,
            Entry(1, 1), Entry(2, 1), Entry(3, 2), Entry(4, 2), Entry(5, 2));
        node.Info.NextIndex[Peer] = 4;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        var req = Assert.Single(transport.SentAppends).Request;
        Assert.Equal(3L, req.PrevLogIndex);
        Assert.Equal(2UL, req.PrevLogTerm); // терм записи 3
        Assert.Equal([4L, 5L], req.Entries.Select(e => e.LogIndex));
    }

    [Fact]
    public async Task NextIndexAtLogEnd_SendsHeartbeatWithLastEntryAsPrev()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 2, Entry(1, 1), Entry(2, 2)); // NextIndex = 3

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        var req = Assert.Single(transport.SentAppends).Request;
        Assert.Equal(2L, req.PrevLogIndex);
        Assert.Equal(2UL, req.PrevLogTerm);
        Assert.Empty(req.Entries);
    }

    [Fact]
    public async Task NextIndexOne_SendsWholeLogWithZeroPrev()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1));
        node.Info.NextIndex[Peer] = 1;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        var req = Assert.Single(transport.SentAppends).Request;
        Assert.Equal(0L, req.PrevLogIndex);
        Assert.Equal(0UL, req.PrevLogTerm);
        Assert.Equal([1L, 2L], req.Entries.Select(e => e.LogIndex));
    }

    [Fact]
    public async Task SendsLeaderCommit()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1));
        node.Info.CommitIndex = 2;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(2L, Assert.Single(transport.SentAppends).Request.LeaderCommit);
    }

    [Fact]
    public async Task SendsOnlyToRequestedPeer()
    {
        var transport = AlwaysSuccess();
        var node = LeaderNode(transport, 1, Entry(1, 1));

        await node.ReplicateToPeerAsync("node3", CancellationToken.None);

        Assert.Equal("node3", Assert.Single(transport.SentAppends).Target);
    }

    // ---------- Разбор ответа ----------

    [Fact]
    public async Task Success_WithEntries_UpdatesMatchAndNext()
    {
        var node = LeaderNode(AlwaysSuccess(), 1,
            Entry(1, 1), Entry(2, 1), Entry(3, 1), Entry(4, 1), Entry(5, 1));
        node.Info.NextIndex[Peer] = 4; // Prev = 3, Entries = [4, 5]

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(5L, node.Info.MatchIndex[Peer]);
        Assert.Equal(6L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task Success_OnHeartbeat_MatchEqualsPrevLogIndex()
    {
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1), Entry(3, 1)); // NextIndex = 4

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(3L, node.Info.MatchIndex[Peer]);
        Assert.Equal(4L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task Success_OnEmptyLog_KeepsZeroAndOne()
    {
        var node = LeaderNode(AlwaysSuccess(), 1);

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(0L, node.Info.MatchIndex[Peer]);
        Assert.Equal(1L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task Success_UpdatesOnlyThatPeer()
    {
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1));
        node.Info.NextIndex[Peer] = 1;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(2L, node.Info.MatchIndex[Peer]);
        Assert.Equal(0L, node.Info.MatchIndex["node3"]);
        Assert.Equal(3L, node.Info.NextIndex["node3"]);
    }

    [Fact]
    public async Task Reject_DecrementsNextIndex_KeepsMatchIndex()
    {
        var node = LeaderNode(AlwaysReject(), 1, Entry(1, 1), Entry(2, 1), Entry(3, 1), Entry(4, 1), Entry(5, 1));
        node.Info.MatchIndex[Peer] = 2;
        // NextIndex = 6

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(5L, node.Info.NextIndex[Peer]);
        Assert.Equal(2L, node.Info.MatchIndex[Peer]);
        Assert.Equal(RaftRole.Leader, node.Info.Role);
    }

    [Fact]
    public async Task Reject_NextIndexNeverDropsBelowOne()
    {
        var node = LeaderNode(AlwaysReject(), 1, Entry(1, 1));
        node.Info.NextIndex[Peer] = 1;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(1L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task HigherTermInResponse_LeaderStepsDown()
    {
        var transport = Responding(_ => new AppendEntriesResp(7, false));
        var node = LeaderNode(transport, 2, Entry(1, 1), Entry(2, 1));
        node.Info.VotedFor = "node1";

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal(7UL, node.Info.CurrentTerm);
        Assert.Equal("", node.Info.VotedFor);
        Assert.Equal("", node.Info.LeaderId);
    }

    [Fact]
    public async Task HigherTermInResponse_DoesNotTouchNextAndMatch()
    {
        var transport = Responding(_ => new AppendEntriesResp(7, false));
        var node = LeaderNode(transport, 2, Entry(1, 1), Entry(2, 1)); // NextIndex = 3

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(3L, node.Info.NextIndex[Peer]);
        Assert.Equal(0L, node.Info.MatchIndex[Peer]);
    }

    [Fact]
    public async Task PeerUnreachable_DoesNotThrow_AndStateUnchanged()
    {
        var transport = new StubTransporter
        {
            OnAppendEntries = (_, _, _) =>
                Task.FromException<AppendEntriesResp>(new HttpRequestException("down"))
        };
        var node = LeaderNode(transport, 2, Entry(1, 1), Entry(2, 1));

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(RaftRole.Leader, node.Info.Role);
        Assert.Equal(2UL, node.Info.CurrentTerm);
        Assert.Equal(3L, node.Info.NextIndex[Peer]);
        Assert.Equal(0L, node.Info.MatchIndex[Peer]);
    }

    // ---------- Конкурентность: ответ пришёл, когда мир уже изменился ----------

    [Fact]
    public async Task Success_UsesRequestSnapshot_NotLeaderLogAtResponseTime()
    {
        // Пока ждали ответ, клиент добавил лидеру записи 3 и 4. Пир подтвердил только до 2.
        var pending = new TaskCompletionSource<AppendEntriesResp>();
        var transport = new StubTransporter { OnAppendEntries = (_, _, _) => pending.Task };
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1));

        var replication = node.ReplicateToPeerAsync(Peer, CancellationToken.None);
        Assert.False(replication.IsCompleted);

        node.Info.LogEntries.AddRange([Entry(3, 1), Entry(4, 1)]);
        pending.SetResult(new AppendEntriesResp(1, true));
        await replication;

        Assert.Equal(2L, node.Info.MatchIndex[Peer]);
        Assert.Equal(3L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task LateSuccess_AfterTermMovedOn_IsIgnored()
    {
        var pending = new TaskCompletionSource<AppendEntriesResp>();
        var transport = new StubTransporter { OnAppendEntries = (_, _, _) => pending.Task };
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1)); // NextIndex = 3

        var replication = node.ReplicateToPeerAsync(Peer, CancellationToken.None);
        Assert.False(replication.IsCompleted);

        node.HandleRequestVote(Vote(5, "node3", lastIndex: 2, lastTerm: 1)); // лидер уступил
        pending.SetResult(new AppendEntriesResp(1, true));
        await replication;

        Assert.Equal(RaftRole.Follower, node.Info.Role);
        Assert.Equal(5UL, node.Info.CurrentTerm);
        Assert.Equal(0L, node.Info.MatchIndex[Peer]);
        Assert.Equal(3L, node.Info.NextIndex[Peer]);
    }

    [Fact]
    public async Task LateReject_AfterTermMovedOn_IsIgnored()
    {
        var pending = new TaskCompletionSource<AppendEntriesResp>();
        var transport = new StubTransporter { OnAppendEntries = (_, _, _) => pending.Task };
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1)); // NextIndex = 3

        var replication = node.ReplicateToPeerAsync(Peer, CancellationToken.None);
        Assert.False(replication.IsCompleted);

        node.HandleRequestVote(Vote(5, "node3", lastIndex: 2, lastTerm: 1));
        pending.SetResult(new AppendEntriesResp(1, false));
        await replication;

        Assert.Equal(3L, node.Info.NextIndex[Peer]); // не уменьшился
    }

    // ---------- Лидер и реальный фолловер вместе ----------

    [Fact]
    public async Task LaggingFollower_IsCaughtUp_AfterSeveralRounds()
    {
        var follower = CreateNode("node2", ["node1", "node3"], new StubTransporter());
        var transport = new StubTransporter
        {
            OnAppendEntries = (_, req, _) => Task.FromResult(follower.HandleAppendEntries(req))
        };
        var leader = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1), Entry(3, 1));
        leader.Info.CommitIndex = 3;

        // Каждый отказ откатывает NextIndex на 1: 4 -> 3 -> 2 -> 1, затем успех.
        for (var i = 0; i < 4; i++)
            await leader.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal([1L, 2L, 3L], follower.Info.LogEntries.Select(e => e.LogIndex));
        Assert.Equal(3L, leader.Info.MatchIndex[Peer]);
        Assert.Equal(4L, leader.Info.NextIndex[Peer]);
        Assert.Equal(3L, follower.Info.CommitIndex);
        Assert.Equal("node1", follower.Info.LeaderId);
    }

    [Fact]
    public async Task FollowerWithConflictingTail_IsOverwrittenByLeader()
    {
        var follower = CreateNode("node2", ["node1", "node3"], new StubTransporter());
        follower.Info.CurrentTerm = 1;
        follower.Info.LogEntries.AddRange([Entry(1, 1), Entry(2, 1), Entry(3, 1)]); // 2 и 3 от старого лидера

        var transport = new StubTransporter
        {
            OnAppendEntries = (_, req, _) => Task.FromResult(follower.HandleAppendEntries(req))
        };
        var leader = LeaderNode(transport, 2, Entry(1, 1), Entry(2, 2), Entry(3, 2));

        for (var i = 0; i < 4; i++)
            await leader.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal([1UL, 2UL, 2UL], follower.Info.LogEntries.Select(e => e.Term));
        Assert.Equal(3L, leader.Info.MatchIndex[Peer]);
    }

    [Fact]
    public async Task Success_AdvancesLeaderCommitIndex_WhenMajorityReached()
    {
        // 3 узла, большинство 2: лидер + node2 имеют записи 1-2
        var node = LeaderNode(AlwaysSuccess(), 1, Entry(1, 1), Entry(2, 1));
        node.Info.NextIndex[Peer] = 1;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(2L, node.Info.CommitIndex);
    }

    [Fact]
    public async Task Success_DoesNotCommitEntriesFromOldTerm()
    {
        // записи терма 1, лидер уже в терме 2: сами они не коммитятся
        var node = LeaderNode(AlwaysSuccess(), 2, Entry(1, 1), Entry(2, 1));
        node.Info.NextIndex[Peer] = 1;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(2L, node.Info.MatchIndex[Peer]);
        Assert.Equal(0L, node.Info.CommitIndex);
    }

    [Fact]
    public async Task Reject_DoesNotChangeCommitIndex()
    {
        var node = LeaderNode(AlwaysReject(), 1, Entry(1, 1), Entry(2, 1));
        node.Info.MatchIndex[Peer] = 2;

        await node.ReplicateToPeerAsync(Peer, CancellationToken.None);

        Assert.Equal(0L, node.Info.CommitIndex);
    }

    [Fact]
    public async Task LateSuccess_DoesNotLowerMatchIndex()
    {
        // Запрос ушёл с записями 1-2. Пока ждали ответ, другой ответ уже поднял MatchIndex до 4.
        var pending = new TaskCompletionSource<AppendEntriesResp>();
        var transport = new StubTransporter { OnAppendEntries = (_, _, _) => pending.Task };
        var node = LeaderNode(transport, 1, Entry(1, 1), Entry(2, 1));
        node.Info.NextIndex[Peer] = 1;

        var replication = node.ReplicateToPeerAsync(Peer, CancellationToken.None);
        Assert.False(replication.IsCompleted);

        node.Info.LogEntries.AddRange([Entry(3, 1), Entry(4, 1)]);
        node.Info.MatchIndex[Peer] = 4;
        node.Info.NextIndex[Peer] = 5;
        pending.SetResult(new AppendEntriesResp(1, true));
        await replication;

        Assert.Equal(4L, node.Info.MatchIndex[Peer]);
        Assert.Equal(5L, node.Info.NextIndex[Peer]);
    }
}