using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Consensus;
using PeerNet.Protocol.Messages.Ledger;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace PeerNet.Protocol.Codecs
{
    public class LedgerRequestCodec : ICodec
    {
        public FrameType FrameType => FrameType.LedgerRequest;

        public IEnvelope Decode(ReadOnlySpan<byte> payload) => new LedgerRequest();
    }

    public class LedgerResponseCodec : ICodec
    {
        public FrameType FrameType => FrameType.LedgerResponse;

        public IEnvelope Decode(ReadOnlySpan<byte> payload)
        {
            var ledger = JsonSerializer.Deserialize<PeerNet.Protocol.Storage.Ledger>(payload) ?? throw new InvalidDataException("Ledger cannot be deserialized");

            return new LedgerResponse(ledger);
        }
    }
}
