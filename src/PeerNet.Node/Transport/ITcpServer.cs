using System.Net;
using System.Net.Sockets;

namespace PeerNet.Node.Transport;

public interface ITcpServer
{
    TcpListener Listener { get; set; }

    void StartServer(IPAddress serverAddress);
}
