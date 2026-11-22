using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Files;
using System.Text.Json;

namespace PeerNet.Protocol.Codecs;

public class FileChunkCodec : ICodec
{
    public FrameType FrameType => FrameType.FileChunk;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return JsonSerializer.Deserialize<FileChunk>(payload);
    }
}
