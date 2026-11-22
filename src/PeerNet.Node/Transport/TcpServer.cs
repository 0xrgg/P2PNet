using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using PeerNet.Protocol.Framing;
using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public class TcpServer : ITcpServer
{
    private readonly IPeerFactory _peerFactory;
    private readonly IPeerRegistry _peerRegistry;
    private readonly ILogger<TcpServer> _logger;

    public TcpServer(IPeerFactory peerFactory, IPeerRegistry peerRegistry, ILogger<TcpServer> logger)
    {
        _peerFactory = peerFactory;
        _peerRegistry = peerRegistry;
        _logger = logger;
    }

    public TcpListener Listener { get; set; }


    public void StartServer(IPAddress serverAddress)
    {
        Listener = new TcpListener(serverAddress, 6969);
        _logger.LogInformation(LogEvents.Setup, "Listening for peers on TCP {Address}:{Port}", serverAddress, 6969);
        Listener.Start();

        var cts = new CancellationTokenSource();
        _ = AcceptClientsAsync(cts.Token);
    }

    private async Task AcceptClientsAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client;

                try
                {
                    client = await Listener.AcceptTcpClientAsync(cancellationToken);
                }
                catch (SocketException ex)
                {
                    // e.g. the client reset before the accept completed: lose that one client, keep listening
                    _logger.LogWarning(LogEvents.Network, "Failed to accept inbound connection: {Reason}", ex.Message);
                    continue;
                }

                // One bad client must not stop the accept loop
                try
                {
                    AcceptClient(client);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(LogEvents.Network, "Failed to set up inbound connection: {Reason}", ex.Message);
                    client.Dispose();
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger.LogError(LogEvents.Network, ex, "TCP server stopped accepting connections");
        }
    }

    private void AcceptClient(TcpClient client)
    {
        _logger.LogInformation(LogEvents.Network, "Inbound connection from {RemoteEndPoint}", client.Client.RemoteEndPoint);

        var peer = _peerFactory.Create(client, (client.Client.RemoteEndPoint as IPEndPoint)?.Address, null);

        peer.Disconnected += p => _peerRegistry.Remove(p.Context.ConnectionId);
        _peerRegistry.Add(peer);

        // HandleConnection never throws; it logs how the connection ended
        _ = peer.HandleConnection();

        // Say hello to establish handshake
        var frame = new FrameBuilder()
            .Create().
            WithType(FrameType.Hello)
            .Build();

        _ = peer.WriteFrameToClient(frame);
    }
}
