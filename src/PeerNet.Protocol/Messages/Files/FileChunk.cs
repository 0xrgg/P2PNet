namespace PeerNet.Protocol.Messages.Files;

public record FileChunk(
    Guid TransferId,
    Guid CorrelationId,
    int Index,
    ReadOnlyMemory<byte> Data) : IEnvelope;
