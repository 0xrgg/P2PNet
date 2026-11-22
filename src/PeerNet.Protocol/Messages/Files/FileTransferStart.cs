namespace PeerNet.Protocol.Messages.Files;

public record FileTransferStart(
    Guid TransferId,
    string FileName,
    long FileSize,
    Guid CorrelationId
    //string HashAlgorithm
    ) : IEnvelope;
