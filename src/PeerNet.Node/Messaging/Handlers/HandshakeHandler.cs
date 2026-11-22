using Microsoft.Extensions.Logging;
using PeerNet.Node.Ledger;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Handshake;

namespace PeerNet.Node.Messaging.Handlers;

/// <summary>Hello / HelloResponse: tells the remote side who we are and learns who they are.</summary>
public sealed class HandshakeHandler : IMessageHandler
{
    private readonly NodeIdentity _identity;
    private readonly IPeerRegistry _registry;
    private readonly ILedgerWorkflow _ledgerWorkflow;
    private readonly ILogger<HandshakeHandler> _logger;

    public HandshakeHandler(NodeIdentity identity, IPeerRegistry registry, ILedgerWorkflow ledgerWorkflow, ILogger<HandshakeHandler> logger)
    {
        _identity = identity;
        _registry = registry;
        _ledgerWorkflow = ledgerWorkflow;
        _logger = logger;
    }

    public IReadOnlyCollection<FrameType> Handles => [FrameType.Hello, FrameType.HelloResponse];

    public async Task HandleAsync(InboundMessage msg, CancellationToken ct)
    {
        var peer = _registry.Get(msg.ConnectionId);
        if (peer is null) return;

        switch (msg.Type)
        {
            case FrameType.Hello:
                var hello = new FrameBuilder()
                    .Create()
                    .WithType(FrameType.HelloResponse)
                    .WithPayload(new HelloResponse(_identity.NodeId))
                    .Build();

                await peer.WriteFrameToClient(hello);
                break;

            case FrameType.HelloResponse:
                var nodeId = ((HelloResponse)msg.Payload!).NodeId;

                peer.Context.UpdateNodeId(nodeId);

                _logger.LogInformation(LogEvents.Network, "Connection {ConnectionId} identified as node {NodeId}", msg.ConnectionId, nodeId);

                // Hand off without awaiting: the ledger responses arrive on this same event loop,
                // so awaiting here would block the loop that has to deliver them
                _ = SyncLedgerAsync(nodeId);
                break;
        }
    }

    private async Task SyncLedgerAsync(Guid nodeId)
    {
        try
        {
            await _ledgerWorkflow.RequestLedgerFromAvailableNode(nodeId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ledger sync with node {NodeId} failed", nodeId);
        }
    }
}
