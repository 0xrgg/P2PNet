using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Files;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace PeerNet.Protocol.Codecs
{
    public class FileRequestCodec : ICodec
    {
        public FrameType FrameType => FrameType.FileRequest;

        public IEnvelope Decode(ReadOnlySpan<byte> payload)
        {
            return JsonSerializer.Deserialize<FileRequest>(payload) ?? throw new InvalidDataException("FileRequest message invalid");
        }
    }
}
