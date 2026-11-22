namespace PeerNet.Protocol.Framing;

public class PeerNetFrame
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public FrameType FrameType { get; set; }
    public object? Payload { get; set; }
    public FrameMetadata? Metadata { get; set; }
}
