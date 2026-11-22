using PeerNet.Node.Transport;

namespace PeerNet.Node.Peers;

public interface IPeerRegistry
{
    IReadOnlyCollection<Peer> GetAll();

    Peer? Get(Guid peerId);
    IEnumerable<Peer> GetByNodeId(Guid nodeId);
    IEnumerable<Peer> GetNodePeersByPeerId(Guid peerId);

    void Add(Peer peer);

    void Remove(Guid peerId);
}
