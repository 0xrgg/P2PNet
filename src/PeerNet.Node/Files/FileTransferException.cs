namespace PeerNet.Node.Files;

/// <summary>
/// An expected, recoverable failure of one file transfer (size or hash mismatch, disk error, unknown transfer).
/// The transfer has already been dropped and its partial file deleted by the time this is thrown, so callers
/// only need to log it and, for a sync fetch, move on to the next holder.
/// </summary>
public sealed class FileTransferException : Exception
{
    public FileTransferException(Guid transferId, string message, Exception? innerException = null)
        : base(message, innerException)
    {
        TransferId = transferId;
    }

    public Guid TransferId { get; }
}
