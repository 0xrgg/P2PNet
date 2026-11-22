using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Transport;
using PeerNet.Protocol.Messages.Discovery;
using System.Collections.Concurrent;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;

namespace PeerNet.Node.Discovery;

public class PeerNetBootstrap : IPeerNetBootstrap
{
    private readonly IPeerConnector _connector;
    private readonly ILogger<PeerNetBootstrap> _logger;

    public PeerNetBootstrap(IPeerConnector connector, ILogger<PeerNetBootstrap> logger)
    {
        _connector = connector;
        _logger = logger;
    }

    private readonly ConcurrentDictionary<IPAddress, DateTime> _knownPeers = new();
    private IPAddress PeerAddress { get; set; }

    private UdpClient UdpClient { get; set; }

    public async Task StartDiscovery(IPAddress peerAddress, Guid nodeId, CancellationToken cancellationToken)
    {
        UdpClient = new UdpClient(new IPEndPoint(IPAddress.Any, 5000));
        _logger.LogInformation(LogEvents.Setup, "Discovery listening on UDP port {Port}", 5000);

        PeerAddress = peerAddress;
        UdpClient.EnableBroadcast = true;

        // Listen for announcements indefinitely in the background
        _ = ListenForPeernetAnnouncements(UdpClient, nodeId, cancellationToken);

        // Run the broadcast for 5 iterations and await its completion
        await BroadcastPeernetDiscovery(UdpClient, nodeId);

        _logger.LogInformation(LogEvents.Network, "Discovery broadcast complete");
    }

    private async Task ListenForPeernetAnnouncements(UdpClient udp, Guid nodeId, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await udp.ReceiveAsync(cancellationToken);
                var address = result.RemoteEndPoint.Address;

                if (address.Equals(PeerAddress))
                    continue;

                UdpFrame? frame;
                try
                {
                    frame = JsonSerializer.Deserialize<UdpFrame>(result.Buffer);
                }
                catch (JsonException)
                {
                    continue;
                }

                if (frame is null)
                    continue;

                switch (frame.Protocol)
                {
                    case UdpProtocol.PeerDiscover:
                        {
                            var response = new UdpFrame { Protocol = UdpProtocol.PeerAnnounce, NodeId = nodeId };
                            byte[] data = JsonSerializer.SerializeToUtf8Bytes(response);
                            await udp.SendAsync(data, result.RemoteEndPoint, cancellationToken);
                            break;
                        }
                    case UdpProtocol.PeerAnnounce:
                        {
                            if (_knownPeers.TryAdd(address, DateTime.UtcNow))
                            {
                                _logger.LogInformation(LogEvents.Network, "Discovered node {NodeId} at {Address}", frame.NodeId, address);

                                // Hand off without awaiting: a slow dial must not block the UDP listener
                                _ = _connector.ConnectAsync(address, frame.NodeId, cancellationToken);
                            }
                            break;
                        }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(LogEvents.Network, ex, "UDP listener error");
            }
        }
    }

    private async Task BroadcastPeernetDiscovery(UdpClient udp, Guid nodeId)
    {
        for (int broadcastRequest = 0; broadcastRequest < 5; broadcastRequest++)
        {
            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (nic.OperationalStatus != OperationalStatus.Up ||
                    nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                    continue;

                foreach (var unicast in nic.GetIPProperties()
                    .UnicastAddresses
                    .Where(x => x.PrefixLength == 24))
                {
                    if (unicast.Address.AddressFamily != AddressFamily.InterNetwork)
                        continue;

                    var ip = unicast.Address.GetAddressBytes();
                    var mask = unicast.IPv4Mask.GetAddressBytes();
                    var broadcast = new byte[4];

                    for (int i = 0; i < 4; i++)
                        broadcast[i] = (byte)(ip[i] | ~mask[i]);

                    var destination = new IPEndPoint(new IPAddress(broadcast), 5000);

                    try
                    {
                        var discoverFrame = new UdpFrame { Protocol = UdpProtocol.PeerDiscover, NodeId = nodeId };
                        await udp.SendAsync(JsonSerializer.SerializeToUtf8Bytes(discoverFrame), destination);
                        _logger.LogDebug(LogEvents.Network, "Discovery broadcast sent to {Destination}", destination);
                    }
                    catch (SocketException ex)
                    {
                        _logger.LogWarning(LogEvents.Network, "Discovery broadcast to {Destination} failed: {Reason}", destination, ex.Message);
                    }
                }
            }

            await Task.Delay(1000);
        }
    }
}
