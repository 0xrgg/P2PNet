using Microsoft.Extensions.Logging;
using PeerNet.Node.Consensus;
using PeerNet.Node.Files;
using PeerNet.Node.Ledger;
using PeerNet.Node.Logging;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Files;

namespace PeerNet.Node.Messaging.Handlers;

/// <summary>FileStart / FileChunk / FileEnd: receives a file, verifies it, then imports (sync) or attests (upload).</summary>
public sealed class FileTransferHandler : IMessageHandler
{
    private readonly FileHandler _files;
    private readonly ILedgerWorkflow _ledgerWorkflow;
    private readonly IAttestationTracker _attestations;
    private readonly ILogger<FileTransferHandler> _logger;

    public FileTransferHandler(FileHandler files, ILedgerWorkflow ledgerWorkflow, IAttestationTracker attestations, ILogger<FileTransferHandler> logger)
    {
        _files = files;
        _ledgerWorkflow = ledgerWorkflow;
        _attestations = attestations;
        _logger = logger;
    }

    public IReadOnlyCollection<FrameType> Handles => [FrameType.FileStart, FrameType.FileChunk, FrameType.FileEnd];

    public Task HandleAsync(InboundMessage msg, CancellationToken ct) => msg.Type switch
    {
        FrameType.FileStart => StartAsync((FileTransferStart)msg.Payload!, msg.ConnectionId),
        FrameType.FileChunk => ChunkAsync((FileChunk)msg.Payload!, msg.ConnectionId, ct),
        FrameType.FileEnd => EndAsync((FileTransferComplete)msg.Payload!, msg.ConnectionId),
        _ => Task.CompletedTask
    };

    private async Task StartAsync(FileTransferStart start, Guid connectionId)
    {
        _logger.LogInformation(LogEvents.Network, "Incoming file {FileName} ({FileSize} bytes), transfer {TransferId}", start.FileName, start.FileSize, start.TransferId);

        try
        {
            if (await _files.HandleFileStartAsync(start, connectionId))
                return;

            // A sync retry reuses the file ID as the transfer ID, so a stalled stream from an earlier holder
            // would block it forever. Drop the stale one when this connection is the one now being fetched from.
            if (_files.GetSourceConnection(start.TransferId) is { } source
                && source != connectionId
                && _ledgerWorkflow.IsFetchSource(start.TransferId, connectionId))
            {
                _logger.LogWarning(LogEvents.Network, "Dropping stalled transfer {TransferId} from connection {StaleConnectionId} so the retry on {ConnectionId} can start", start.TransferId, source, connectionId);

                await _files.DropAsync(start.TransferId);

                if (await _files.HandleFileStartAsync(start, connectionId))
                    return;
            }

            _logger.LogWarning(LogEvents.Network, "Ignoring duplicate FileStart for active transfer {TransferId} on connection {ConnectionId}", start.TransferId, connectionId);
        }
        catch (FileTransferException ex)
        {
            ReportDropped(ex, connectionId);
        }
    }

    private async Task ChunkAsync(FileChunk chunk, Guid connectionId, CancellationToken ct)
    {
        try
        {
            await _files.HandleFileChunkAsync(chunk.TransferId, connectionId, chunk.Index, chunk.Data, ct);
        }
        catch (FileTransferException ex)
        {
            ReportDropped(ex, connectionId);
        }
    }

    private async Task EndAsync(FileTransferComplete end, Guid connectionId)
    {
        FileReceivedNotification? file;

        // End of a stream already dropped (and logged) mid-transfer: nothing left to verify
        if (_files.TryClearDropped(end.TransferId))
        {
            _logger.LogDebug(LogEvents.Network, "Stream for dropped transfer {TransferId} ended on connection {ConnectionId}", end.TransferId, connectionId);
            _ledgerWorkflow.FailFetch(end.TransferId, connectionId);
            return;
        }

        try
        {
            // Throws on size or hash mismatch, so only a verified copy is imported or attested
            file = await _files.HandleFileCompleteAsync(end, connectionId, _ledgerWorkflow.GetExpectedChecksum(end.TransferId));
        }
        catch (FileTransferException ex)
        {
            ReportDropped(ex, connectionId);
            return;
        }
        catch
        {
            // Lets a sync fetch move straight to the next holder instead of waiting for its timeout
            _ledgerWorkflow.FailFetch(end.TransferId, connectionId);
            throw;
        }

        if (file is null)
        {
            _logger.LogWarning(LogEvents.Network, "Ignoring FileEnd for transfer {TransferId} from non-source connection {ConnectionId}", end.TransferId, connectionId);
            return;
        }

        _logger.LogInformation(LogEvents.Network, "File transfer {TransferId} complete and verified", file.FileTransferId);

        // A synced file was already accepted elsewhere: import its ledger entry, don't cast a new vote
        if (_ledgerWorkflow.TryCompleteSyncFetch(file.FileTransferId, connectionId, file.Checksum))
            return;

        await _attestations.AttestLocalAsync(file.FileTransferId, file.Checksum);
    }

    /// <summary>
    /// Logs a transfer that <see cref="FileHandler"/> has already dropped and cleaned up. If it was a sync
    /// fetch, the fetch moves straight on to its next attempt instead of waiting for its timeout.
    /// </summary>
    private void ReportDropped(FileTransferException ex, Guid connectionId)
    {
        _logger.LogWarning(LogEvents.Network, "Dropped file transfer {TransferId} from connection {ConnectionId}: {Reason}", ex.TransferId, connectionId, ex.Message);

        _ledgerWorkflow.FailFetch(ex.TransferId, connectionId);
    }
}
