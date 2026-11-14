using NeoCask.Raft.DTO;

namespace NeoCask.Raft.Interfaces;

public interface INeoCaskRaftTransporter
{
    public Task<RequestVoteResp> RequestVoteAsync(string targetNodeId, RequestVoteReq requestVoteReq,
        CancellationToken cancellationToken);

    public Task<AppendEntriesResp> AppendEntriesAsync(string targetNodeId, AppendEntriesReq appendEntriesReq,
        CancellationToken cancellationToken);
}