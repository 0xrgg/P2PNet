using PeerNet.Protocol.Framing;

namespace PeerNet.Node.Messaging;

/// <summary>Handles one feature's frames. Register with DI as <c>IMessageHandler</c>; <see cref="EventDispatcher"/> routes by <see cref="Handles"/>.</summary>
public interface IMessageHandler
{
    IReadOnlyCollection<FrameType> Handles { get; }
    Task HandleAsync(InboundMessage msg, CancellationToken ct);
}
