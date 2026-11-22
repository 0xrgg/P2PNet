using System.Net;

namespace PeerNet.Node.Discovery
{
    public interface IPeerNetBootstrap
    {
        Task StartDiscovery(IPAddress peerAddress, Guid nodeId, CancellationToken cancellationToken);
    }
}