using System.Net.Http.Json;
using NeoCask.Raft.DTO;
using NeoCask.Raft.Interfaces;

namespace NeoCask.Raft;

public interface IRaftNodeAddressProvider
{
    Uri GetAddress(string nodeId);
}

public class InvalidDataException(string message = $"Node returned empty response") : Exception();

public class NeoCaskRaftTransporter(HttpClient httpClient, IRaftNodeAddressProvider nodeAddressProvider)
    : INeoCaskRaftTransporter
{
    public async Task<RequestVoteResp> RequestVoteAsync(string targetNodeId, RequestVoteReq requestVoteReq,
        CancellationToken cancellationToken)
    {
        var result = await Send<RequestVoteReq, RequestVoteResp>(targetNodeId, "request-vote", requestVoteReq,
            cancellationToken);

        return result;
    }

    public async Task<AppendEntriesResp> AppendEntriesAsync(string targetNodeId, AppendEntriesReq appendEntriesReq,
        CancellationToken cancellationToken)
    {
        var result = await Send<AppendEntriesReq, AppendEntriesResp>(targetNodeId, "append-entries", appendEntriesReq,
            cancellationToken);

        return result;
    }

    private async Task<TResponse> Send<TRequest, TResponse>(string targetNodeId, string endpoint, TRequest request,
        CancellationToken cancellationToken)
    {
        var nodeAddress = nodeAddressProvider.GetAddress(targetNodeId);
        var requestUri = new Uri(nodeAddress, endpoint);

        using var response = await httpClient.PostAsJsonAsync(requestUri, request, cancellationToken);

        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<TResponse>(cancellationToken) ??
               throw new InvalidDataException($"Node {targetNodeId} returned empty response");
    }
}