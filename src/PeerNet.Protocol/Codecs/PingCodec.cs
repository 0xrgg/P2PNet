using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Heartbeat;

namespace PeerNet.Protocol.Codecs;

public class PingCodec : ICodec
{
    public FrameType FrameType => FrameType.Ping;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return new Ping();
    }
}
