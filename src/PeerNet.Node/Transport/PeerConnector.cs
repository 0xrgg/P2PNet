using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public sealed class PeerConnector : IPeerConnector
{
    private readonly IPeerFactory _peerFactory;
    private readonly IPeerRegistry _registry;
    private readonly ILogger<PeerConnector> _logger;

    // Node IDs with a dial in flight, so repeated discoveries don't open parallel connections
    private readonly ConcurrentDictionary<Guid, byte> _dialling = new();

    public PeerConnector(IPeerFactory peerFactory, IPeerRegistry registry, ILogger<PeerConnector> logger)
    {
        _peerFactory = peerFactory;
        _registry = registry;
        _logger = logger;
    }

    public async Task ConnectAsync(IPAddress address, Guid nodeId, CancellationToken ct)
    {
        if (ct.IsCancellationRequested)
            return;

        if (_registry.GetByNodeId(nodeId).Any())
            return;

        if (!_dialling.TryAdd(nodeId, 0))
            return;

        var client = new TcpClient();

        try
        {
            var peer = _peerFactory.Create(client, address, nodeId);

            await peer.StartConnection();

            // Register only once connected, so failed dials don't leave dead peers in the registry,
            // and remove on disconnect so closed connections don't linger there either
            peer.Disconnected += p => _registry.Remove(p.Context.ConnectionId);
            _registry.Add(peer);

            _logger.LogInformation(LogEvents.Network, "Connected to node {NodeId} at {Address} (connection {ConnectionId})", nodeId, address, peer.Context.ConnectionId);

            // HandleConnection never throws; it logs how the connection ended
            _ = peer.HandleConnection();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            client.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(LogEvents.Network, "Connection to node {NodeId} at {Address} failed: {Reason}", nodeId, address, ex.Message);
            client.Dispose();
        }
        finally
        {
            _dialling.TryRemove(nodeId, out _);
        }
    }
}
