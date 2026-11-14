namespace NeoCask.Raft.DTO;

public record NeoCaskRaftClusterConfig(string NodeId, IReadOnlyList<string> PeersIds);