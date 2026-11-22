using PeerNet.Protocol.Messages.Files;
using System.Collections.Concurrent;
using System.IO.Compression;

namespace PeerNet.Node.Files;

/// <summary>
/// Receives file transfers. Expected failures (size or hash mismatch, disk errors, frames for an unknown
/// transfer) drop the transfer, delete its partial file and throw <see cref="FileTransferException"/>;
/// any remaining frames of a dropped stream are then ignored quietly instead of failing one by one.
/// </summary>
/// <remarks>Called only from the single event loop, so transfers are never handled concurrently.</remarks>
public class FileHandler
{
    private readonly ConcurrentDictionary<Guid, FileReceiveState> _activeTransfers = new();

    // Transfers dropped mid-stream; cleared by that stream's FileEnd or by a new FileStart for the same ID
    private readonly ConcurrentDictionary<Guid, byte> _dropped = new();

    /// <summary>
    /// Starts receiving a transfer from <paramref name="connectionId"/>. Returns false, and changes nothing,
    /// if the transfer is already active (a duplicate stream), so it cannot reopen or truncate the file being written.
    /// </summary>
    public async Task<bool> HandleFileStartAsync(FileTransferStart start, Guid connectionId)
    {
        if (_activeTransfers.ContainsKey(start.TransferId))
            return false;

        // A new stream for a previously dropped transfer (a retry) starts clean
        _dropped.TryRemove(start.TransferId, out _);

        string basePath = NodePaths.ReceivedFiles;

        string filePath = Path.Combine(
            basePath,
            start.TransferId.ToString());

        FileStream output;

        try
        {
            Directory.CreateDirectory(basePath);

            output = new FileStream(
                filePath,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                81920,
                useAsync: true);
        }
        catch (Exception ex) when (IsExpectedFileFailure(ex))
        {
            _dropped.TryAdd(start.TransferId, 0);
            throw new FileTransferException(start.TransferId, $"Could not create {filePath}: {ex.Message}", ex);
        }

        var transfer = new FileReceiveState
        {
            CorrelationId = start.CorrelationId,
            TransferId = start.TransferId,
            SourceConnectionId = connectionId,
            ExpectedSize = start.FileSize,
            FilePath = filePath,
            FileName = Path.GetFileName(start.FileName),
            Output = output
        };

        if (!_activeTransfers.TryAdd(start.TransferId, transfer))
        {
            await transfer.DisposeAsync();
            return false;
        }

        return true;
    }

    /// <summary>The connection an active transfer is being received from, or null if it isn't active.</summary>
    public Guid? GetSourceConnection(Guid transferId)
        => _activeTransfers.TryGetValue(transferId, out var transfer) ? transfer.SourceConnectionId : null;

    public async Task HandleFileChunkAsync(
        Guid transferId,
        Guid connectionId,
        int chunkIndex,
        ReadOnlyMemory<byte> data,
        CancellationToken ct)
    {
        if (!_activeTransfers.TryGetValue(transferId, out var transfer))
        {
            if (_dropped.ContainsKey(transferId))
                return;                                   // rest of a stream that was already dropped and reported

            // Report the first orphan chunk only; the rest of the stream is then ignored as dropped
            _dropped.TryAdd(transferId, 0);
            throw new FileTransferException(transferId, "Received a chunk for a transfer that was never started.");
        }

        // A chunk from a second stream for the same transfer would corrupt the file
        if (transfer.SourceConnectionId != connectionId)
            return;

        Exception? failure = null;

        await transfer.Lock.WaitAsync(ct);

        try
        {
            if (transfer.IsCompleted)
                throw new InvalidOperationException("Transfer already completed.");

            if (transfer.BytesReceived + data.Length > transfer.ExpectedSize)
                throw new InvalidDataException("Received more data than expected.");

            transfer.Hasher.AppendData(data.Span);

            await transfer.Output.WriteAsync(data, ct);

            transfer.BytesReceived += data.Length;
        }
        catch (Exception ex) when (IsExpectedFileFailure(ex))
        {
            failure = ex;
        }
        finally
        {
            transfer.Lock.Release();
        }

        if (failure is not null)
        {
            await DropAsync(transferId);
            throw new FileTransferException(transferId, failure.Message, failure);
        }
    }

    /// <summary>
    /// Completes and verifies a transfer. Returns null, leaving the transfer untouched, if the FileEnd
    /// came from a connection other than the one that started it.
    /// </summary>
    public async Task<FileReceivedNotification?> HandleFileCompleteAsync(FileTransferComplete complete, Guid connectionId, string? expectedChecksum = null)
    {
        if (!_activeTransfers.TryGetValue(complete.TransferId, out var transfer))
            throw new FileTransferException(complete.TransferId, "Received FileEnd for a transfer that was never started.");

        if (transfer.SourceConnectionId != connectionId)
            return null;

        string actualHashHex;
        string? zipPath = null;

        await transfer.Lock.WaitAsync();

        try
        {
            if (transfer.IsCompleted)
                throw new InvalidOperationException("Transfer already completed.");

            transfer.IsCompleted = true;

            await transfer.Output.FlushAsync();

            if (transfer.BytesReceived != transfer.ExpectedSize)
                throw new InvalidDataException($"File size mismatch: received {transfer.BytesReceived} of {transfer.ExpectedSize} bytes.");

            byte[] actualHash = transfer.Hasher.GetHashAndReset();

            actualHashHex = Convert.ToHexString(actualHash);

            // A synced file is checked against the ledger's checksum, not the sender's own claim
            if (!actualHashHex.Equals(
                expectedChecksum ?? complete.FileHash,
                StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException("File hash mismatch.");
            }

            await transfer.Output.DisposeAsync();

            // zip the file
            zipPath = Path.ChangeExtension(transfer.FilePath, ".zip");
            await ZipReceivedFileAsync(transfer.FilePath, zipPath, transfer.FileName);

            // delete the original
            File.Delete(transfer.FilePath);
        }
        catch (Exception ex)
        {
            await transfer.Output.DisposeAsync();
            TryDelete(transfer.FilePath);

            // A half-written zip would later be served to peers as if it were the file
            if (zipPath is not null)
                TryDelete(zipPath);

            if (IsExpectedFileFailure(ex))
                throw new FileTransferException(complete.TransferId, ex.Message, ex);

            throw;
        }
        finally
        {
            _activeTransfers.TryRemove(complete.TransferId, out _);

            transfer.Lock.Release();
            await transfer.DisposeAsync();
        }

        var file = new FileReceivedNotification()
        {
            FileTransferId = transfer.TransferId,
            CorrelationId = transfer.CorrelationId,
            Checksum = actualHashHex,
            Path = transfer.FilePath
        };

        return file;
    }

    /// <summary>
    /// If <paramref name="transferId"/> was dropped mid-stream, forgets it and returns true.
    /// Called on that stream's FileEnd, after which no more frames for it are expected.
    /// </summary>
    public bool TryClearDropped(Guid transferId) => _dropped.TryRemove(transferId, out _);

    /// <summary>
    /// Abandons an active transfer: closes its file, deletes the partial data and ignores the rest of its stream.
    /// </summary>
    public async Task DropAsync(Guid transferId)
    {
        _dropped.TryAdd(transferId, 0);

        if (!_activeTransfers.TryRemove(transferId, out var transfer))
            return;

        await transfer.DisposeAsync();
        TryDelete(transfer.FilePath);
    }

    // Failures caused by the data or the disk, as opposed to bugs; these drop the transfer rather than fault
    private static bool IsExpectedFileFailure(Exception ex)
        => ex is IOException or InvalidDataException or UnauthorizedAccessException or InvalidOperationException;

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch (IOException) { }                  // still locked; it is overwritten by the next attempt anyway
        catch (UnauthorizedAccessException) { }
    }

    private static async Task ZipReceivedFileAsync(
        string filePath,
        string zipPath,
        string fileName,
        CancellationToken ct = default)
    {
        await using var zipStream = new FileStream(
            zipPath,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            81920,
            useAsync: true);

        using var archive = new ZipArchive(
            zipStream,
            ZipArchiveMode.Create,
            leaveOpen: true);

        var entry = archive.CreateEntry(
            fileName,
            CompressionLevel.Optimal);

        await using var entryStream = entry.Open();
        await using var inputStream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            81920,
            useAsync: true);

        await inputStream.CopyToAsync(entryStream, ct);
    }
}
