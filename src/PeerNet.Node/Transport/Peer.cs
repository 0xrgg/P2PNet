using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Messaging;
using PeerNet.Protocol.Codecs;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using System.Net.Sockets;
using System.Threading.Channels;

namespace PeerNet.Node.Transport;

public class Peer
{
    private readonly IDecoder _decoder;
    private readonly IEventBus _eventBus;
    private readonly ILogger<Peer> _logger;

    // Ping/Pong traffic logs under its own category (PeerNet.Node.Transport.HeartBeat) so its
    // level can be set in config independently of other network events.
    private readonly ILogger<HeartBeat> _heartbeatLogger;

    private Channel<PeerNetFrame> _writeChannel = Channel.CreateUnbounded<PeerNetFrame>();

    public ConnectionContext Context { get; set; }
    public HeartBeat HeartBeat { get; set; } = new();

    private readonly PeerNetFrame Pong = new FrameBuilder().Create().WithType(FrameType.Pong).Build();

    public Peer(
        ConnectionContext context,
        IDecoder decoder,
        IEventBus eventBus,
        ILogger<Peer> logger,
        ILogger<HeartBeat> heartbeatLogger)
    {
        Context = context;
        _decoder = decoder;
        _eventBus = eventBus;
        _logger = logger;
        _heartbeatLogger = heartbeatLogger;
    }

    /// <summary>
    /// Raised once when the connection has closed, for whatever reason, after its loops have stopped.
    /// Owners use it to remove the peer from the registry.
    /// </summary>
    public event Action<Peer>? Disconnected;

    /// <summary>
    /// Runs the connection until it closes. Never throws: a connection ending is a normal event in a
    /// peer-to-peer network, so the cause is logged here and <see cref="Disconnected"/> is raised.
    /// </summary>
    public async Task HandleConnection()
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Context.ConnectionCancellationToken);

        try
        {
            var client = Context.Client;

            _logger.LogInformation(LogEvents.Network, "Starting connection handler {ConnectionId} for {RemoteEndPoint}", Context.ConnectionId, client.Client.RemoteEndPoint);

            using (client)
            using (NetworkStream stream = client.GetStream())
            {
                Task[] loops =
                [
                    TcpClientReadLoop(stream, cts),
                    PingLoop(cts),
                    WriteDataLoop(cts.Token),
                ];

                // Whichever loop stops first is why the connection is closing; the others are just stopped
                var first = await Task.WhenAny(loops);

                cts.Cancel();

                try
                {
                    await Task.WhenAll(loops);
                }
                catch
                {
                    // Every loop's exception is observed here; only the first loop's is the cause
                }

                LogConnectionEnded(first.Exception?.GetBaseException());
            }
        }
        catch (Exception ex)
        {
            // Setting up the stream failed, e.g. the socket closed between accept/connect and here
            LogConnectionEnded(ex);
        }
        finally
        {
            // Later writes fail fast with PeerDisconnectedException instead of queueing into a dead connection
            _writeChannel.Writer.TryComplete();

            try
            {
                Disconnected?.Invoke(this);
            }
            catch (Exception ex)
            {
                _logger.LogError(LogEvents.Network, ex, "Disconnected handler failed for connection {ConnectionId}", Context.ConnectionId);
            }
        }
    }

    private void LogConnectionEnded(Exception? cause)
    {
        switch (cause)
        {
            case null or OperationCanceledException:
                _logger.LogInformation(LogEvents.Network, "Connection {ConnectionId} closed", Context.ConnectionId);
                break;

            // The remote end went away or the socket was torn down: routine, no stack trace needed
            case IOException or SocketException or ObjectDisposedException:
                _logger.LogInformation(LogEvents.Network, "Connection {ConnectionId} ended: {Reason}", Context.ConnectionId, cause.Message);
                break;

            // A frame with a bad length prefix means the stream can't be resynchronised
            case InvalidDataException:
                _logger.LogWarning(LogEvents.Network, "Connection {ConnectionId} dropped after an invalid frame: {Reason}", Context.ConnectionId, cause.Message);
                break;

            default:
                _logger.LogError(LogEvents.Network, cause, "Connection {ConnectionId} failed unexpectedly", Context.ConnectionId);
                break;
        }
    }

    private async Task WriteDataLoop(CancellationToken ct)
    {
        await foreach (var frame in _writeChannel.Reader.ReadAllAsync(ct))
        {
            var rawFrame = FrameSerializer.Serialize(frame);

            await Context.Client.Client.SendAsync(rawFrame, ct);
        }
    }

    public async Task StartConnection()
    {
        await Context.Client.ConnectAsync(Context.IpAddress, 6969, Context.ConnectionCancellationToken);
    }

    private async Task PingLoop(CancellationTokenSource cts)
    {
        while (!cts.IsCancellationRequested)
        {
            await Task.Delay(5000, cts.Token);

            var ping = HeartBeat.StartPing();
            _heartbeatLogger.LogTrace(LogEvents.Network, "Ping sent to {RemoteEndPoint}", Context.RemoteEndpoint);

            var pingFrame = new FrameBuilder()
                .Create()
                .WithType(FrameType.Ping)
                .Build();

            await _writeChannel.Writer.WriteAsync(pingFrame, cts.Token);

            try
            {
                await ping.Task.WaitAsync(
                    TimeSpan.FromSeconds(15),
                    cts.Token);

                _heartbeatLogger.LogTrace(LogEvents.Network, "Pong received from {RemoteEndPoint}", Context.RemoteEndpoint);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(LogEvents.Network, "Ping to {RemoteEndPoint} expired; closing connection {ConnectionId}", Context.RemoteEndpoint, Context.ConnectionId);

                Context.Client.Dispose();
                cts.Cancel();
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                // Expected shutdown; no further action needed.
            }
            catch (Exception ex)
            {
                _logger.LogWarning(LogEvents.Network, "Ping on connection {ConnectionId} failed: {Reason}", Context.ConnectionId, ex.Message);

                Context.Client.Dispose();
                cts.Cancel();
            }
            finally
            {
                HeartBeat.Cancel(ping);
            }
        }
    }

    private async Task TcpClientReadLoop(NetworkStream stream, CancellationTokenSource cts)
    {
        while (!cts.IsCancellationRequested)
        {
            var message = await stream.ReceiveFrameAsync(cts.Token);

            switch (message.Type)
            {
                // Connection-level frames are answered here; everything else goes to the node's handlers
                case FrameType.Ping:
                    _heartbeatLogger.LogTrace(LogEvents.Network, "Ping received on connection {ConnectionId}; replying", Context.ConnectionId);
                    await _writeChannel.Writer.WriteAsync(Pong, cts.Token);
                    break;
                case FrameType.Pong:
                    HeartBeat.PongReceived();
                    break;
                default:
                    IEnvelope decoded;
                    try
                    {
                        decoded = _decoder.Decode(message);
                    }
                    catch (Exception ex)
                    {
                        // Frames are length-prefixed, so one bad payload doesn't desync the stream: skip it
                        _logger.LogError(ex, "Failed to decode {FrameType} frame {FrameId}", message.Type, message.FrameId);
                        break;
                    }

                    _logger.LogDebug(LogEvents.Network, "Received {FrameType} frame {FrameId} on connection {ConnectionId}", message.Type, message.FrameId, Context.ConnectionId);

                    await _eventBus.PublishAsync(new InboundMessage(Context.ConnectionId, message.FrameId, message.Type, decoded, message.Metadata), cts.Token);
                    break;
            }
        }
    }

    /// <summary>
    /// Queues a frame for sending. Throws <see cref="PeerDisconnectedException"/> if the connection has closed.
    /// </summary>
    public Task WriteFrameToClient(PeerNetFrame frame)
    {
        // The channel is unbounded, so TryWrite only fails once it has been completed on disconnect
        if (!_writeChannel.Writer.TryWrite(frame))
            throw new PeerDisconnectedException(Context.ConnectionId);

        return Task.CompletedTask;
    }

}
