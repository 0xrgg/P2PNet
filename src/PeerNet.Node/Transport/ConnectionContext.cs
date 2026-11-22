using PeerNet.Protocol.Messages;
using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public class ConnectionContext
{
    public Guid ConnectionId { get; init; } = Guid.NewGuid();
    public Guid? NodeId { get; private set; }
    public EndPoint? RemoteEndpoint { get; private set; }
    public IPAddress IpAddress { get; private set; }
    public TcpClient Client { get; set; }
    public CancellationTokenSource ConnectionCancellationTokenSource { get; private set; }
    public CancellationToken ConnectionCancellationToken => ConnectionCancellationTokenSource.Token;

    public ConnectionContext(TcpClient client, IPAddress address, Guid? nodeId)
    {
        Client = client;
        RemoteEndpoint = client.Client.RemoteEndPoint;
        IpAddress = address;
        ConnectionCancellationTokenSource = new();
        NodeId = nodeId;
    }

    public void UpdateNodeId(Guid nodeId) => NodeId = nodeId;
}
