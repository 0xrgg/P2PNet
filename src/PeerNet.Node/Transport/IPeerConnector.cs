using System.Net;

namespace PeerNet.Node.Transport;

/// <summary>
/// Establishes outbound connections to remote nodes. Knows nothing about how the
/// node was found, so discovery, seed lists or reconnect logic can all call it.
/// </summary>
public interface IPeerConnector
{
    Task ConnectAsync(IPAddress address, Guid nodeId, CancellationToken ct);
}
