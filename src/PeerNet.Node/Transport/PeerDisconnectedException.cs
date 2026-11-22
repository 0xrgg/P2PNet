namespace PeerNet.Node.Transport;

/// <summary>
/// Thrown when a frame is written to a peer whose connection has already closed. It derives from
/// <see cref="IOException"/> so callers that already treat I/O failures as "peer gone" handle it the same way.
/// </summary>
public sealed class PeerDisconnectedException : IOException
{
    public PeerDisconnectedException(Guid connectionId)
        : base($"Connection {connectionId} is closed.")
    {
        ConnectionId = connectionId;
    }

    public Guid ConnectionId { get; }
}
