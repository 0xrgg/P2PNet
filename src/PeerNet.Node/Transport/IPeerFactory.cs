using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public interface IPeerFactory
{
    Peer Create(TcpClient client, IPAddress ipAddress, Guid? nodeId);
}
