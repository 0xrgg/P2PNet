using PeerNet.Node.Transport;
using System.Collections.Concurrent;

namespace PeerNet.Node.Peers;

public class PeerRegistry : IPeerRegistry
{
    private readonly ConcurrentDictionary<Guid, Peer> _peers = new();

    public IReadOnlyCollection<Peer> GetAll()
    {
        return _peers.Values.ToArray();
    }

    public Peer? Get(Guid peerId)
    {
        return _peers.TryGetValue(peerId, out var peer)
            ? peer
            : null;
    }

    public IEnumerable<Peer> GetByNodeId(Guid nodeId)
    {
        return _peers.Where(x => x.Value.Context.NodeId == nodeId).Select(x => x.Value);
    }

    public void Add(Peer peer)
    {
        _peers[peer.Context.ConnectionId] = peer;
    }

    public void Remove(Guid peerId)
    {
        _peers.TryRemove(peerId, out _);
    }

    public IEnumerable<Peer> GetNodePeersByPeerId(Guid peerId)
    {
        if (!_peers.TryGetValue(peerId, out var peer))
            return Enumerable.Empty<Peer>();

        var nodeId = peer.Context.NodeId;

        return _peers.Values
            .Where(x => x.Context.NodeId == nodeId);
    }
}
