namespace PeerNet.Node.Consensus;

public sealed class AttestationOptions
{
    /// <summary>
    /// Distinct nodes (the sender included) that must attest the same checksum before a file
    /// is accepted. Fixed rather than a majority of N, so nodes never need to agree on network size.
    /// Use 2 for two-machine tests.
    /// </summary>
    public int ReplicationFactor { get; set; } = 3;
}
