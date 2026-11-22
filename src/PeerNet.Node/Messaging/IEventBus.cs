namespace PeerNet.Node.Messaging;

public interface IEventBus
{
    ValueTask PublishAsync(InboundMessage message, CancellationToken cancellationToken = default);
    IAsyncEnumerable<InboundMessage> ReadAllAsync(CancellationToken cancellationToken = default);
}
