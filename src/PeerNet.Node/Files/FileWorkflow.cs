using Microsoft.Extensions.Logging;
using PeerNet.Node.Consensus;
using PeerNet.Node.Logging;
using PeerNet.Node.Messaging;
using PeerNet.Node.Peers;
using PeerNet.Node.Transport;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Files;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Security.Cryptography;

namespace PeerNet.Node.Files;

public class FileWorkflow : IFileWorkflow
{
    private readonly IPeerRegistry _registry;
    private readonly IAttestationTracker _attestations;
    private readonly ILogger<FileWorkflow> _logger;

    public FileWorkflow(IEventBus eventBus, IPeerRegistry registry, IAttestationTracker attestations, ILogger<FileWorkflow> logger)
    {
        _registry = registry;
        _attestations = attestations;
        _logger = logger;
    }


    // fileID, peer
    public ConcurrentDictionary<Guid, List<Peer>> PendingFiles { get; set; } = new();
    public ConcurrentBag<Guid> ClientIdsAwaitingResponse { get; set; } = new();

    public async Task Execute(Guid fileId, string filePath,CancellationToken ct)
    {
        // a request needs creating and waiting on

        try
        {
            await SendFile(fileId, filePath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or FileTransferException)
        {
            // Dropped, not fatal: peers that missed the file pick it up through ledger sync once it is accepted
            _logger.LogWarning(LogEvents.Network, "File broadcast failed: {Reason}", ex.Message);
        }

    }

    public async Task RequestFile(Guid fileId, Guid nodeId, CancellationToken ct)
    {
        var frame = new FrameBuilder().Create().WithType(FrameType.FileRequest).WithPayload(new FileRequest(fileId, nodeId)).Build();

        await Parallel.ForEachAsync(_registry.GetAll(), parallelOptions: new ParallelOptions { CancellationToken = ct, MaxDegreeOfParallelism = 4 }, async (peer, ct) => 
        {
            await peer.WriteFrameToClient(frame);
        });
    }

    // TODO: Consolidate similar methods here
    /// <summary>Serves a requested file back on the one connection the request arrived on.</summary>
    public async Task ServeFile(Guid fileId, Peer requester)
    {
        try
        {
            var peers = new List<Peer> { requester };

            var correlationId = Guid.NewGuid();

            string basePath = NodePaths.ReceivedFiles;

            string zipPath = Path.Combine(
                basePath,
                $"{fileId.ToString()}.zip");

            if (!File.Exists(zipPath))
            {
                _logger.LogWarning(LogEvents.Network, "Connection {ConnectionId} requested {FileId}, which this node doesn't hold", requester.Context.ConnectionId, fileId);
                return;
            }

            await using var fileStream = await OpenWithRetryAsync(zipPath);
            using var archive = new ZipArchive(fileStream, ZipArchiveMode.Read);

            if (archive.Entries.Count != 1)
                throw new InvalidDataException(
                    $"Expected exactly one file in ZIP, found {archive.Entries.Count}.");

            var entry = archive.Entries[0];

            await using var entryStream = entry.Open();

            using var ms = new MemoryStream();

            await entryStream.CopyToAsync(ms);

            ms.Position = 0;

            PendingFiles.TryAdd(fileId, peers);

            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

            const int chunkSize = 4096;

            byte[] fileBuf = new byte[chunkSize];
            long offset = 0;

            var metadata = new FrameMetadata() { CorrelationId = correlationId };

            var fileStart = new FileTransferStart(fileId, entry.Name, entry.Length, correlationId);

            // write the file start to all

            //var fileStartFrame = PeerNetFrame.Create(fileStart, FrameType.FileStart, metadata);

            var fileStartFrame = new FrameBuilder()
                .Create()
                .WithType(FrameType.FileStart)
                .WithPayload(fileStart)
                .WithMetadata(metadata)
                .Build();

            await WriteAll(peers, fileStartFrame, fileId);

            foreach (var peer in peers)
            {
                ClientIdsAwaitingResponse.Add(peer.Context.ConnectionId);
            }

            // start chunking the file and send to all
            int chunkIndex = 1;

            while (offset < ms.Length)
            {
                metadata.GenerateNewFrameId();

                int bytesToRead = (int)Math.Min(chunkSize, ms.Length - offset);

                await ms.ReadExactlyAsync(
                    fileBuf.AsMemory(0, bytesToRead),
                    default);

                hasher.AppendData(fileBuf, 0, bytesToRead);

                var chunkData = fileBuf.AsMemory(0, bytesToRead).ToArray();
                // Process or send only bytesToRead bytes.
                var chunk = new FileChunk(fileId, correlationId, chunkIndex, chunkData);

                var chunkFrame = new FrameBuilder()
                    .Create()
                    .WithType(FrameType.FileChunk)
                    .WithPayload(chunk)
                    .WithMetadata(metadata)
                    .Build();

                await WriteAll(peers, chunkFrame, fileId);

                offset += bytesToRead;
                chunkIndex++;
            }

            byte[] fileHash = hasher.GetHashAndReset();

            string hashHex = Convert.ToHexString(fileHash);

            // finally, send file stop with the hash to all peers

            var fileEnd = new FileTransferComplete(fileId, correlationId, hashHex);

            metadata.GenerateNewFrameId();

            var fileEndFrame = new FrameBuilder()
                .Create()
                .WithType(FrameType.FileEnd)
                .WithPayload(fileEnd)
                .WithMetadata(metadata)
                .Build();

            await WriteAll(peers, fileEndFrame, fileId);

            // The sender holds the original, so it counts as one of the R attestations.
            await _attestations.AttestLocalAsync(fileId, hashHex);

        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or FileTransferException)
        {
            // Drop this serve: the requester's fetch times out or is failed and it retries with another holder
            _logger.LogWarning(LogEvents.Network, "Could not serve {FileId} to connection {ConnectionId}: {Reason}", fileId, requester.Context.ConnectionId, ex.Message);
        }

    }

    private async Task SendFile(Guid fileId, string filePath)
    {
        var peers = _registry.GetAll().ToList();

        _logger.LogDebug("Attempting to submit file: {filePath}", filePath);

        if (peers.Count == 0)
        {
            _logger.LogWarning(LogEvents.Network, "No connected peers to send the file to");
            return;
        }

        var correlationId = Guid.NewGuid();
        var fileTransmissionId = fileId;

        PendingFiles.TryAdd(fileTransmissionId, peers);

        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        // if the file is large, it needs chunking, framing and then writing
        using var file = await OpenWithRetryAsync(filePath);// "Patterns of Enterprise Application Architecture.pdf");

        const int chunkSize = 4096;

        byte[] fileBuf = new byte[chunkSize];
        long offset = 0;

        var metadata = new FrameMetadata() { CorrelationId = correlationId };

        var fileStart = new FileTransferStart(fileTransmissionId, file.Name, file.Length, correlationId);

        // write the file start to all

        //var fileStartFrame = PeerNetFrame.Create(fileStart, FrameType.FileStart, metadata);

        var fileStartFrame = new FrameBuilder()
            .Create()
            .WithType(FrameType.FileStart)
            .WithPayload(fileStart)
            .WithMetadata(metadata)
            .Build();

        await WriteAll(peers, fileStartFrame, fileTransmissionId);

        foreach(var peer in peers)
        {
            ClientIdsAwaitingResponse.Add(peer.Context.ConnectionId);
        }

        // start chunking the file and send to all
        int chunkIndex = 1;

        while (offset < file.Length)
        {
            metadata.GenerateNewFrameId();

            int bytesToRead = (int)Math.Min(chunkSize, file.Length - offset);

            await file.ReadExactlyAsync(
                fileBuf.AsMemory(0, bytesToRead),
                default);

            hasher.AppendData(fileBuf, 0, bytesToRead);

            var chunkData = fileBuf.AsMemory(0, bytesToRead).ToArray();
            // Process or send only bytesToRead bytes.
            var chunk = new FileChunk(fileTransmissionId, correlationId, chunkIndex, chunkData);

            var chunkFrame = new FrameBuilder()
                .Create()
                .WithType(FrameType.FileChunk)
                .WithPayload(chunk)
                .WithMetadata(metadata)
                .Build();

            await WriteAll(peers, chunkFrame, fileTransmissionId);

            offset += bytesToRead;
            chunkIndex++;
        }

        byte[] fileHash = hasher.GetHashAndReset();

        string hashHex = Convert.ToHexString(fileHash);

        // finally, send file stop with the hash to all peers

        var fileEnd = new FileTransferComplete(fileTransmissionId, correlationId, hashHex);

        metadata.GenerateNewFrameId();

        var fileEndFrame = new FrameBuilder()
            .Create()
            .WithType(FrameType.FileEnd)
            .WithPayload(fileEnd)
            .WithMetadata(metadata)
            .Build();

        await WriteAll(peers, fileEndFrame, fileTransmissionId);

        // The sender holds the original, so it counts as one of the R attestations.
        await _attestations.AttestLocalAsync(fileTransmissionId, hashHex);
    }

    /// <summary>
    /// Writes a frame to every peer still in <paramref name="peers"/>. A peer whose connection has closed is
    /// dropped from the list (and so from the rest of this transfer) and logged; the others carry on.
    /// Throws <see cref="FileTransferException"/> once no recipients remain.
    /// </summary>
    private async Task WriteAll(List<Peer> peers, PeerNetFrame frame, Guid fileId)
    {
        foreach (var peer in peers.ToArray())
        {
            try
            {
                await peer.WriteFrameToClient(frame);
            }
            catch (PeerDisconnectedException)
            {
                peers.Remove(peer);
                _logger.LogWarning(LogEvents.Network, "Connection {ConnectionId} closed during transfer of {FileId}; dropped it from this transfer", peer.Context.ConnectionId, fileId);
            }
        }

        if (peers.Count == 0)
            throw new FileTransferException(fileId, "Every recipient disconnected.");
    }

    private const int MaxOpenAttempts = 3;

    /// <summary>
    /// Opens a file for reading, retrying briefly if it is locked (e.g. still being written or zipped).
    /// A missing file is not retried.
    /// </summary>
    private async Task<FileStream> OpenWithRetryAsync(string path)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return File.OpenRead(path);
            }
            catch (IOException ex) when (ex is not FileNotFoundException and not DirectoryNotFoundException && attempt < MaxOpenAttempts)
            {
                _logger.LogWarning(LogEvents.Network, "Could not open {Path} (attempt {Attempt} of {MaxAttempts}): {Reason}; retrying", path, attempt, MaxOpenAttempts, ex.Message);
                await Task.Delay(TimeSpan.FromSeconds(attempt));
            }
        }
    }
}
