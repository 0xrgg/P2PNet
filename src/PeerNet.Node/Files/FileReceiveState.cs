using System.Security.Cryptography;

namespace PeerNet.Node.Files;

public class FileReceiveState : IAsyncDisposable
{
    public Guid CorrelationId { get; set; }
    public Guid TransferId { get; set; }

    /// <summary>The connection that sent FileStart; frames for this transfer from any other connection are dropped.</summary>
    public Guid SourceConnectionId { get; set; }
    public long ExpectedSize { get; set; }
    public long BytesReceived { get; set; }
    public string FilePath { get; set; }
    public string FileName { get; set; }

    public FileStream Output { get; set; }
    public IncrementalHash Hasher { get; set; } =
        IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

    public SemaphoreSlim Lock { get; } = new(1, 1);

    public bool IsCompleted { get; set; }

    public async ValueTask DisposeAsync()
    {
        await Output.DisposeAsync();
        Hasher.Dispose();
        Lock.Dispose();
    }
}
