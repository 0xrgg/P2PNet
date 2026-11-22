using PeerNet.Protocol.Framing;

namespace PeerNet.Node.Messaging;

/// <summary>A decoded frame as received on one of this node's connections. Node-internal; never sent on the wire.</summary>
public sealed record InboundMessage(Guid ConnectionId, Guid FrameId, FrameType Type, object? Payload, FrameMetadata Metadata);
