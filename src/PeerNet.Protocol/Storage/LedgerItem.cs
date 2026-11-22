namespace PeerNet.Protocol.Storage;

public class LedgerItem
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public Guid ObjectId { get; set; }
    public required string Checksum { get; set; }
    public required string PreviousHash { get; set; }
    public required string EntryHash { get; set; }
    public List<Guid> Attestations { get; set; } = [];
    public DateTimeOffset Created { get; init; } = DateTimeOffset.UtcNow;
}
