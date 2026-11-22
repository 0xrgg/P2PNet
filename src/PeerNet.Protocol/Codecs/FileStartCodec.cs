using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Files;
using System.Text.Json;

namespace PeerNet.Protocol.Codecs;

public class FileStartCodec : ICodec
{
    public FrameType FrameType => FrameType.FileStart;

    public IEnvelope Decode(ReadOnlySpan<byte> payload)
    {
        return JsonSerializer.Deserialize<FileTransferStart>(payload);
    }
}
