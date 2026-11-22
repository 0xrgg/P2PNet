using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PeerNet.Protocol.Messages.Ledger
{
    public record LedgerRequest : IEnvelope;

    public record LedgerResponse(PeerNet.Protocol.Storage.Ledger Ledger) : IEnvelope;

}
