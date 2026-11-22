using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Heartbeat;

namespace PeerNet.Protocol.Codecs;

public class PongCodec : ICodec
{
    public FrameType FrameType => FrameType.Pong;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return new Pong();
    }
}
