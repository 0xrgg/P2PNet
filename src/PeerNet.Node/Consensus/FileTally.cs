namespace PeerNet.Node.Consensus;

public enum TallyResult
{
    /// <summary>A new attestation was recorded; the file is not (newly) accepted.</summary>
    Counted,
    /// <summary>This node already attested the same checksum.</summary>
    Duplicate,
    /// <summary>This node already attested a different checksum; the new one is ignored.</summary>
    Conflict,
    /// <summary>This attestation took the file to the replication factor. Returned exactly once per file.</summary>
    Accepted
}

/// <summary>
/// Attestations for one file: one checksum per node, first one wins.
/// </summary>
public sealed class FileTally
{
    private readonly object _lock = new();
    private readonly Dictionary<Guid, string> _checksumsByNode = new();
    private readonly int _replicationFactor;

    /// <summary>The nodes whose matching attestations caused acceptance. Empty until accepted; fixed after.</summary>
    public IReadOnlyList<Guid> AcceptedBy { get; private set; } = [];

    public FileTally(int replicationFactor)
    {
        if (replicationFactor < 1)
            throw new ArgumentOutOfRangeException(nameof(replicationFactor));

        _replicationFactor = replicationFactor;
    }

    public string? AcceptedChecksum { get; private set; }

    public bool IsAccepted => AcceptedChecksum is not null;

    public int Count
    {
        get { lock (_lock) return _checksumsByNode.Count; }
    }

    /// <summary>
    /// This will contain ALL attestations (i.e. even conflicting ones) and has been superceded.
    /// </summary>
    public IEnumerable<Guid> AttestingNodes { get { lock (_lock) return _checksumsByNode.Keys.ToArray(); } }

    public TallyResult Add(Guid nodeId, string checksum)
    {
        // Convert.ToHexString emits upper case; normalise anything else that arrives.
        var normalized = checksum.ToUpperInvariant();

        lock (_lock)
        {
            if (_checksumsByNode.TryGetValue(nodeId, out var existing))
            {
                return existing == normalized ? TallyResult.Duplicate : TallyResult.Conflict;
            }

            _checksumsByNode[nodeId] = normalized;

            if (AcceptedChecksum is null)
            {
                var agreeing = _checksumsByNode
                    .Where(kv => kv.Value == normalized)
                    .Select(kv => kv.Key)
                    .ToArray();

                if (agreeing.Length >= _replicationFactor)
                {
                    AcceptedBy = agreeing;
                    AcceptedChecksum = normalized;
                    return TallyResult.Accepted;
                }

            }

            return TallyResult.Counted;
        }
    }
}
