using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PeerNet.Protocol.Messages.Files
{
    public record FileRequest(Guid FileId, Guid RequestingNodeId) : IEnvelope;
}
