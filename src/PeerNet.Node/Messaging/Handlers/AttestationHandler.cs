using Microsoft.Extensions.Logging;
using PeerNet.Node.Consensus;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Consensus;

namespace PeerNet.Node.Messaging.Handlers;

/// <summary>Attestation: records another node's vote for a file's checksum.</summary>
public sealed class AttestationHandler : IMessageHandler
{
    private readonly NodeIdentity _identity;
    private readonly IPeerRegistry _registry;
    private readonly IAttestationTracker _attestations;
    private readonly ILogger<AttestationHandler> _logger;

    public AttestationHandler(NodeIdentity identity, IPeerRegistry registry, IAttestationTracker attestations, ILogger<AttestationHandler> logger)
    {
        _identity = identity;
        _registry = registry;
        _attestations = attestations;
        _logger = logger;
    }

    public IReadOnlyCollection<FrameType> Handles => [FrameType.Attestation];

    public Task HandleAsync(InboundMessage msg, CancellationToken ct)
    {
        var attestation = (FileAttestation)msg.Payload!;

        ArgumentException.ThrowIfNullOrWhiteSpace(attestation.Checksum);

        if (attestation.NodeId == _identity.NodeId)
        {
            _logger.LogWarning(LogEvents.Consensus, "Dropping attestation for file {FileId} claiming to be from this node, received on connection {ConnectionId}", attestation.FileId, msg.ConnectionId);
            return Task.CompletedTask;
        }

        if (NodeIdentity.DeriveNodeId(attestation.PublicKey) != attestation.NodeId)
        {
            _logger.LogWarning(LogEvents.Consensus, "Dropping attestation for file {FileId}: public key does not match node {NodeId}", attestation.FileId, attestation.NodeId);
            return Task.CompletedTask;
        }

        var data = NodeIdentity.BuildSigningPayload(attestation.FileId, attestation.Checksum, attestation.NodeId);

        var isVerifiedPayload = NodeIdentity.Verify(attestation.PublicKey, data, attestation.Payload);

        if (!NodeIdentity.Verify(attestation.PublicKey, data, attestation.Payload))
        {
            _logger.LogWarning(LogEvents.Consensus, "Dropping attestation for file {FileId}: invalid signature from node {NodeId}", attestation.FileId, attestation.NodeId);
            return Task.CompletedTask;
        }

        _attestations.Record(attestation);
        return Task.CompletedTask;
    }
}
