using PeerNet.Protocol.Messages.Consensus;

namespace PeerNet.Node.Consensus;

public interface IAttestationTracker
{
    Dictionary<Guid, FileTally> GetTrackerTallies { get; }

    /// <summary>Counts this node's own attestation and broadcasts it to every connected peer.</summary>
    Task AttestLocalAsync(Guid fileId, string checksum);

    /// <summary>Records an attestation; writes to the ledger the first time the file is accepted.</summary>
    TallyResult Record(FileAttestation attestation);
}
