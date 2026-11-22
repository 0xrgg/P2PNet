namespace PeerNet.Protocol.Messages.Discovery;

public class UdpFrame
{
    public UdpProtocol Protocol { get; set; }
    public Guid NodeId { get; set; }
}
