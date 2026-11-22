using Microsoft.Extensions.Logging;
using PeerNet.Node.Peers;
using PeerNet.Node.Storage;
using PeerNet.Protocol.Storage;
using System.Collections.Concurrent;

namespace PeerNet.Node.Files
{
    public class Outbox
    {
        private readonly ILogger<Outbox> _logger;
        private readonly IFileWorkflow _fileWorkflow;
        private readonly IPeerRegistry _peers;

        private static readonly TimeSpan AcceptTimeout = TimeSpan.FromMinutes(5);
        private const int MaxAttempts = 3;
        private readonly ConcurrentDictionary<Guid, OutboxEntry> _entries = new();

        public Outbox(ILogger<Outbox> logger, IFileWorkflow fileWorkflow, ILedgerService ledgerService, IPeerRegistry peers)
        {
            _logger = logger;
            _fileWorkflow = fileWorkflow;
            _peers = peers;

            ledgerService.ItemAdded += OnItemAdded;
        }

        public async Task RunAsync(CancellationToken ct)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
            do
            {
                try
                {
                    Scan();
                    await ProcessAsync(ct);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Outbox pass failed");   // log and keep going; one bad file shouldn't stop the loop
                }
            }
            while (await timer.WaitForNextTickAsync(ct));
        }

        public void Scan()
        {
            Directory.CreateDirectory(NodePaths.DropZone);

            foreach(var path in Directory.EnumerateFiles(NodePaths.DropZone))
            {
                if (_entries.Values.Any(e => e.Path == path)) continue;

                var entry = new OutboxEntry { FileId = Guid.NewGuid(), Path =  path };
                _entries.TryAdd(entry.FileId, entry);
            }
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            if (!_peers.GetAll().Any()) return;

            foreach (var entry in _entries.Values)
            {
                var due = entry.State == OutboxState.Queued
                    || (entry.State == OutboxState.AwaitingAcceptance && DateTimeOffset.UtcNow - entry.SentAt > AcceptTimeout);

                if (!due) continue;

                if (entry.Attempts >= MaxAttempts) { MoveTo(entry, NodePaths.DropZoneFailed); continue; }

                entry.State = OutboxState.Sending;
                entry.Attempts++;
                await _fileWorkflow.Execute(entry.FileId, entry.Path, ct);
                entry.SentAt = DateTimeOffset.UtcNow;
                entry.State = OutboxState.AwaitingAcceptance;
            }
        }

        private void OnItemAdded(LedgerItem item)
        {
            if (!_entries.TryRemove(item.ObjectId, out var entry)) return;   // not ours
            _logger.LogInformation("Outbox file {Path} accepted as {FileId}", entry.Path, item.ObjectId);
            MoveTo(entry, NodePaths.DropZonePublished);
            // other post-accept actions go here
        }

        private static void MoveTo(OutboxEntry e, string dir)
        {
            Directory.CreateDirectory(dir);
            File.Move(e.Path, Path.Combine(dir, $"{e.FileId}_{Path.GetFileName(e.Path)}"), overwrite: true);
        }
    }

    public enum OutboxState { Queued, Sending, AwaitingAcceptance }

    public sealed class OutboxEntry
    {
        public required Guid FileId { get; init; }
        public required string Path { get; init; }
        public OutboxState State { get; set; } = OutboxState.Queued;
        public DateTimeOffset? SentAt { get; set; }
        public int Attempts { get; set; }
    }
}
