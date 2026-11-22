using System.Threading.Channels;

namespace PeerNet.Node.Messaging;

public class EventBus : IEventBus
{
    private readonly Channel<InboundMessage> _channel =
        Channel.CreateUnbounded<InboundMessage>();

    public ValueTask PublishAsync(
        InboundMessage message,
        CancellationToken cancellationToken = default)
    {
        return _channel.Writer.WriteAsync(
            message,
            cancellationToken);
    }

    public IAsyncEnumerable<InboundMessage> ReadAllAsync(
        CancellationToken cancellationToken = default)
    {
        return _channel.Reader.ReadAllAsync(cancellationToken);
    }
}
