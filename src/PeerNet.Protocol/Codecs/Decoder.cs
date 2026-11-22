using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;

namespace PeerNet.Protocol.Codecs;

public sealed class Decoder : IDecoder
{
    private readonly IReadOnlyDictionary<FrameType, ICodec>
        _codecs;

    public Decoder(IEnumerable<ICodec> codecs)
    {
        _codecs = codecs.ToDictionary(
            codec => codec.FrameType);
    }

    public IEnvelope Decode(PeerNetMessage msg)
    {
        if (!_codecs.TryGetValue(
                msg.Type,
                out var codec))
        {
            throw new InvalidDataException(
                $"Unknown message type: {msg.Type}");
        }

        return codec.Decode(msg.Payload.ToArray());
    }
}
