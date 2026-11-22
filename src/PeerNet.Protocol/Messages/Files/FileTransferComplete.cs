namespace PeerNet.Protocol.Messages.Files;

public record FileTransferComplete(
    Guid TransferId,
    Guid CorrelationId,
    string FileHash) : IEnvelope;
