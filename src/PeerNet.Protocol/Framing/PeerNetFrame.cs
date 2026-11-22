namespace PeerNet.Protocol.Framing;

public class PeerNetFrame
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public byte[] Header { get; set; } = [];
    public FrameType FrameType { get; set; }
    public object? Payload { get; set; }
    public FrameMetadata? Metadata { get; set; }

    // TODO: Tighten this portion up to mark as sent in the channel
    public TaskCompletionSource<bool>? Sent { get; }
}
