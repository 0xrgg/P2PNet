using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Handshake;
using System.Text.Json;

namespace PeerNet.Protocol.Codecs;

public class HelloResponseCodec : ICodec
{
    public FrameType FrameType => FrameType.HelloResponse;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return JsonSerializer.Deserialize<HelloResponse>(payload);
    }
}
