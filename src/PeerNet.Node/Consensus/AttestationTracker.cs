using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using PeerNet.Node.Storage;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Consensus;
using System.Collections.Concurrent;

namespace PeerNet.Node.Consensus;

/// <summary>
/// Leaderless attest-and-count. Every node holding a verified copy broadcasts a
/// <see cref="FileAttestation"/>; each node keeps its own tally and accepts the file once
/// <see cref="AttestationOptions.ReplicationFactor"/> distinct nodes agree on the checksum.
/// Acceptance is final, so nodes can lag but never diverge.
/// </summary>
public sealed class AttestationTracker : IAttestationTracker
{
    private readonly ConcurrentDictionary<Guid, FileTally> _tallies = new();

    private readonly NodeIdentity _identity;
    private readonly AttestationOptions _options;
    private readonly IPeerRegistry _registry;
    private readonly ILedgerService _ledger;
    private readonly ILogger<AttestationTracker> _logger;

    public Dictionary<Guid, FileTally> GetTrackerTallies => _tallies.ToDictionary();

    public AttestationTracker(
        NodeIdentity identity,
        AttestationOptions options,
        IPeerRegistry registry,
        ILedgerService ledger,
        ILogger<AttestationTracker> logger)
    {
        if (options.ReplicationFactor < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "ReplicationFactor must be at least 1.");

        _identity = identity;
        _options = options;
        _registry = registry;
        _ledger = ledger;
        _logger = logger;
    }

    public async Task AttestLocalAsync(Guid fileId, string checksum)
    {

        var payload = NodeIdentity.BuildSigningPayload(fileId, checksum, _identity.NodeId);
        var attestation = new FileAttestation(fileId, checksum, _identity.NodeId, _identity.PublicKey, _identity.Sign(payload));

        Record(attestation);

        // One frame for every connection: a node reached over two connections sees the
        // same FrameId twice and the node-level dedup drops the second.
        var frame = new FrameBuilder()
            .Create()
            .WithType(FrameType.Attestation)
            .WithPayload(attestation)
            .Build();

        foreach (var peer in _registry.GetAll())
        {
            try
            {
                await peer.WriteFrameToClient(frame);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(LogEvents.Consensus, "Failed to send attestation for file {FileId} to connection {ConnectionId}: {Reason}", fileId, peer.Context.ConnectionId, ex.Message);
            }
        }
    }

    public TallyResult Record(FileAttestation attestation)
    {
        var tally = _tallies.GetOrAdd(attestation.FileId, _ => new FileTally(_options.ReplicationFactor));

        var result = tally.Add(attestation.NodeId, attestation.Checksum);

        switch (result)
        {
            case TallyResult.Counted:
                _logger.LogDebug(LogEvents.Consensus, "Node {NodeId} attested file {FileId} ({Checksum})", attestation.NodeId, attestation.FileId, attestation.Checksum);
                break;

            case TallyResult.Duplicate:
                _logger.LogTrace(LogEvents.Consensus, "Duplicate attestation from node {NodeId} for file {FileId}", attestation.NodeId, attestation.FileId);
                break;

            case TallyResult.Conflict:
                _logger.LogWarning(LogEvents.Consensus, "Node {NodeId} sent a conflicting checksum {Checksum} for file {FileId}; ignored", attestation.NodeId, attestation.Checksum, attestation.FileId);
                break;

            case TallyResult.Accepted:
                _logger.LogInformation(LogEvents.Consensus, "File {FileId} accepted with {ReplicationFactor} matching attestations ({Checksum})", attestation.FileId, _options.ReplicationFactor, tally.AcceptedChecksum);
                var newItem = _ledger.Add(tally.AcceptedChecksum!, attestation.FileId, tally.AcceptedBy);

                break;
        }

        return result;
    }
}
