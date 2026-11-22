using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;

namespace PeerNet.Protocol.Codecs;

public interface IDecoder
{
    IEnvelope Decode(PeerNetMessage message);
}
