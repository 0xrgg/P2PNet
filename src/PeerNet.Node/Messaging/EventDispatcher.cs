using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Transport;
using PeerNet.Protocol.Framing;

namespace PeerNet.Node.Messaging;

/// <summary>
/// Routes each inbound frame to the one <see cref="IMessageHandler"/> registered for its <see cref="FrameType"/>.
/// </summary>
public sealed class EventDispatcher
{
    private const int RecentFrameLimit = 500;

    private readonly Dictionary<FrameType, IMessageHandler> _handlers = new();
    private readonly ILogger<EventDispatcher> _logger;

    // Short record of recent frames, so one received on several connections (duplex) is processed once
    private readonly HashSet<Guid> _recentFrameSet = new();
    private readonly Queue<Guid> _recentFrameQueue = new();

    public EventDispatcher(IEnumerable<IMessageHandler> handlers, ILogger<EventDispatcher> logger)
    {
        _logger = logger;

        foreach (var handler in handlers)
        foreach (var type in handler.Handles)
        {
            // Fail at startup rather than silently dropping one of the two handlers
            if (!_handlers.TryAdd(type, handler))
                throw new InvalidOperationException($"{type} is handled by both {_handlers[type].GetType().Name} and {handler.GetType().Name}");
        }
    }

    public async Task DispatchAsync(InboundMessage msg, CancellationToken ct)
    {
        if (!TryRegisterFrame(msg.FrameId))
        {
            _logger.LogDebug(LogEvents.Network, "Discarding duplicate frame {FrameId}", msg.FrameId);
            return;
        }

        if (!_handlers.TryGetValue(msg.Type, out var handler))
        {
            _logger.LogWarning(LogEvents.Network, "No handler for {FrameType} from {ConnectionId}", msg.Type, msg.ConnectionId);
            return;
        }

        try
        {
            await handler.HandleAsync(msg, ct);
        }
        catch (PeerDisconnectedException ex)
        {
            // The handler tried to reply to (or forward to) a peer that has since gone: routine, not a fault
            _logger.LogInformation(LogEvents.Network, "Dropped {FrameType} from {ConnectionId}: connection {ClosedConnectionId} closed while handling it", msg.Type, msg.ConnectionId, ex.ConnectionId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Failed to process {FrameType} from {ConnectionId}", msg.Type, msg.ConnectionId);
        }
    }

    // Only called from the single event loop, so no lock is needed
    private bool TryRegisterFrame(Guid frameId)
    {
        if (!_recentFrameSet.Add(frameId))
            return false;

        _recentFrameQueue.Enqueue(frameId);

        while (_recentFrameQueue.Count > RecentFrameLimit)
            _recentFrameSet.Remove(_recentFrameQueue.Dequeue());

        return true;
    }
}
