using PeerNet.Node.Transport;
using PeerNet.Protocol.Messages.Files;
using System.Collections.Concurrent;

namespace PeerNet.Node.Files;

public interface IFileWorkflow
{
    ConcurrentDictionary<Guid, List<Peer>> PendingFiles { get; set; }
    Task ServeFile(Guid fileId, Peer requester);
    Task RequestFile(Guid fileId, Guid nodeId, CancellationToken ct);

    Task Execute(Guid fileId, string filePath, CancellationToken ct);
}
