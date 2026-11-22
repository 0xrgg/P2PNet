using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Handshake;

namespace PeerNet.Protocol.Codecs;

public class HelloCodec : ICodec
{
    public FrameType FrameType => FrameType.Hello;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return new Hello();
    }
}
