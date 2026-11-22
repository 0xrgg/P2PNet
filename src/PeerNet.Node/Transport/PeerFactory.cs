using Microsoft.Extensions.Logging;
using PeerNet.Node.Logging;
using PeerNet.Node.Messaging;
using PeerNet.Protocol.Codecs;
using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public sealed class PeerFactory : IPeerFactory
{
    private readonly IDecoder _decoder;
    private readonly IEventBus _eventBus;
    private readonly ILogger<Peer> _peerLogger;
    private readonly ILogger<HeartBeat> _heartbeatLogger;

    public PeerFactory(IDecoder decoder, IEventBus eventBus, ILoggerFactory loggerFactory)
    {
        _decoder = decoder;
        _eventBus = eventBus;
        _peerLogger = loggerFactory.CreateLogger<Peer>();
        _heartbeatLogger = loggerFactory.CreateLogger<HeartBeat>();
    }

    public Peer Create(TcpClient client, IPAddress ipAddress, Guid? nodeId)
    {
        var address = ipAddress ?? (client.Client.RemoteEndPoint as IPEndPoint)?.Address
            ?? throw new InvalidOperationException(
                "Unable to determine the remote IP address.");

        var context = new ConnectionContext(client, address, nodeId);

        return new Peer(context, _decoder, _eventBus, _peerLogger, _heartbeatLogger);
    }
}
