namespace PeerNet.Protocol.Framing;

public class FrameMetadata
{
    public Guid CorrelationId { get; init; }
    public Guid FrameId { get; private set; } = Guid.NewGuid();
    public Guid NodeId { get; set; }

    public void GenerateNewFrameId() => FrameId = Guid.NewGuid();
}
