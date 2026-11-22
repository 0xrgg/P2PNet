using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages;
using PeerNet.Protocol.Messages.Consensus;
using System.Text.Json;

namespace PeerNet.Protocol.Codecs;

public sealed class FileAttestationCodec : ICodec
{
    public FrameType FrameType => FrameType.Attestation;

    public IEnvelope Decode(ReadOnlySpan<byte> payload) =>
        JsonSerializer.Deserialize<FileAttestation>(payload)
        ?? throw new InvalidDataException("Invalid attestation payload");
}
