namespace PeerNet.Protocol.Framing;

public sealed record PeerNetMessage(
    FrameType Type,
    FrameMetadata Metadata,
    ReadOnlyMemory<byte> Payload,
    Guid FrameId);
