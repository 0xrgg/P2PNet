using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;

namespace PeerNet.Protocol.Codecs;

public interface ICodec
{
    FrameType FrameType { get; }

    IEnvelope Decode(ReadOnlySpan<byte> payload);
}
