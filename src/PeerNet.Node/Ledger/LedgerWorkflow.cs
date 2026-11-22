using Microsoft.Extensions.Logging;
using PeerNet.Node.Consensus;
using PeerNet.Node.Logging;
using PeerNet.Node.Peers;
using PeerNet.Node.Storage;
using PeerNet.Node.Transport;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Storage;
using System.Collections.Concurrent;
using System.Text.Json;

namespace PeerNet.Node.Ledger
{
    public class LedgerWorkflow : ILedgerWorkflow
    {
        private readonly IPeerRegistry _peerRegistry;
        private readonly NodeIdentity _nodeIdentity;
        private readonly ILogger<LedgerWorkflow> _logger;
        private readonly ILedgerService _ledgerService;
        private readonly AttestationOptions _attestationOptions;

        private ConcurrentDictionary<Guid, LedgerRequest> _pendingLedgerRequests = new();

        public LedgerWorkflow(
            IPeerRegistry peerRegistry,
            NodeIdentity nodeIdentity,
            ILogger<LedgerWorkflow> logger,
            ILedgerService ledgerService,
            AttestationOptions attestationOptions)
        {
            _peerRegistry = peerRegistry;
            _nodeIdentity = nodeIdentity;
            _logger = logger;
            _ledgerService = ledgerService;
            _attestationOptions = attestationOptions;
        }

        public async Task RequestLedgerFromAvailableNode(Guid nodeId)
        {
            // A node usually has two connections (both sides dial), so HelloResponse can trigger this twice
            if (!_syncingNodes.TryAdd(nodeId, 0))
            {
                _logger.LogDebug("Ledger sync with node {NodeId} already running, skipping", nodeId);
                return;
            }

            try
            {
                _logger.LogDebug("Requesting ledger from node {NodeId}", nodeId);

                await SyncFromAsync(await SendLedgerRequestsAsync(_peerRegistry.GetByNodeId(nodeId)));
            }
            finally { _syncingNodes.TryRemove(nodeId, out _); }
        }

        private readonly ConcurrentDictionary<Guid, byte> _syncingNodes = new();

        private int _syncRunning;
        private int _syncRequested;

        public async Task RequestLedgersFromAvailableNodes()
        {
            Interlocked.Exchange(ref _syncRequested, 1);

            while (Volatile.Read(ref _syncRequested) == 1 && Interlocked.CompareExchange(ref _syncRunning, 1, 0) == 0)
            {
                try
                {
                    while (Interlocked.Exchange(ref _syncRequested, 0) == 1)
                        await SyncFromAsync(await SendLedgerRequestsAsync(_peerRegistry.GetAll()));
                }
                finally { Interlocked.Exchange(ref _syncRunning, 0); }
            }
        }

        private async Task SyncFromAsync((Protocol.Storage.Ledger ledger, Guid connectionId)[] responses)
        {
            Console.WriteLine($"Ledger sync complete - received {responses.Length} responses");

            _logger.LogInformation("Ledger contents:");
            foreach (var response in responses)
            {
                _logger.LogInformation(JsonSerializer.Serialize(response.ledger));
            }

            var existingFileIds = _ledgerService.Get.Items.ToList().Select(x => x.ObjectId).ToHashSet();
            existingFileIds.ExceptWith(_needsRepair.Keys);

            List<NodeView> nodes = GetNodeList(responses);

            var targets = BuildSyncTargets(nodes, existingFileIds);

            // One source per file, a few files at a time; FetchAsync falls back to the next holder on failure
            await Parallel.ForEachAsync(
                targets,
                new ParallelOptions { MaxDegreeOfParallelism = MaxConcurrentFetches },
                (target, ct) => new ValueTask(FetchAsync(target, ct)));
        }

        private List<NodeView> GetNodeList((Protocol.Storage.Ledger ledger, Guid connectionId)[] responses)
        {
            return _peerRegistry.GetAll()
                .GroupBy(x => x.Context.NodeId)
                .Select(group =>
                {
                    var connections = group.ToHashSet();

                    var response = responses.FirstOrDefault(
                        r => connections.Any(c => c.Context.ConnectionId == r.connectionId));

                    return new NodeView(group.Key, response.ledger, connections);
                })
                .ToList();
        }

        private readonly ConcurrentDictionary<Guid, LedgerItem> _needsRepair = new();

        public void MarkForRepair(LedgerItem item)
        {
            if (_needsRepair.TryAdd(item.ObjectId, item))
                _logger.LogWarning("File {FileId} marked for repair", item.ObjectId);

        }

        private static readonly TimeSpan LedgerResponseTimeout = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Sends a ledger request on each connection and waits for those responses only. A connection that
        /// doesn't answer within <see cref="LedgerResponseTimeout"/> is skipped rather than stalling the sync.
        /// </summary>
        private async Task<(Protocol.Storage.Ledger ledger, Guid connectionId)[]> SendLedgerRequestsAsync(IEnumerable<Peer> peers)
        {
            var sent = new List<KeyValuePair<Guid, LedgerRequest>>();

            // One ledger per node: ask on a single connection per node. 
            // Connections still mid-handshake have no NodeId yet and are skipped.
            var onePerNode = peers
                .Where(p => p.Context.NodeId is not null)
                .GroupBy(p => p.Context.NodeId)
                .Select(g => g.First());

            try
            {
                foreach (var peer in onePerNode)
                {
                    var correlationId = Guid.NewGuid();

                    var entry = new KeyValuePair<Guid, LedgerRequest>(correlationId, new LedgerRequest(peer.Context.ConnectionId));

                    _pendingLedgerRequests[entry.Key] = entry.Value;
                    sent.Add(entry);

                    var ledgerRequest = new FrameBuilder()
                        .Create()
                        .WithType(FrameType.LedgerRequest)
                        .WithMetadata(new() { NodeId = _nodeIdentity.NodeId, CorrelationId = correlationId })
                        .Build();

                    await peer.WriteFrameToClient(ledgerRequest);
                }

                var results = await Task.WhenAll(sent.Select(async entry =>
                {
                    try
                    {
                        return (Ok: true, Response: await entry.Value.Result.Task.WaitAsync(LedgerResponseTimeout));
                    }
                    catch (TimeoutException)
                    {
                        _logger.LogWarning("No ledger response for {CorrelationId} within {Timeout}", entry.Key, LedgerResponseTimeout);
                        return (Ok: false, Response: default((Protocol.Storage.Ledger ledger, Guid connectionId)));
                    }
                }));

                return results.Where(r => r.Ok).Select(r => r.Response).ToArray();
            }
            finally
            {
                // Remove only this call's entries; a newer request on the same connection stays registered
                foreach (var entry in sent)
                    _pendingLedgerRequests.TryRemove(entry);
            }
        }

        private const int MaxConcurrentFetches = 4;
        private static readonly TimeSpan FetchTimeout = TimeSpan.FromMinutes(2);

        // fileId -> the single connection the file was requested on, and the signal for its outcome
        private readonly ConcurrentDictionary<Guid, InFlightFetch> _inFlight = new();

        // fileId -> what the file is expected to be, for the whole FetchAsync loop (not just one attempt),
        // so a transfer that arrives after its attempt timed out is still imported rather than attested as new
        private readonly ConcurrentDictionary<Guid, SyncTarget> _syncing = new();

        private async Task FetchAsync(SyncTarget target, CancellationToken ct)
        {
            var fileId = target.Expected.ObjectId;
            if (!_syncing.TryAdd(fileId, target)) return;      // already being fetched

            try
            {
                for (int round = 1; round <= MaxFetchRounds; round++)
                {
                    if (round > 1)
                    {
                        // Back off between passes so a holder that was briefly unavailable can recover or reconnect
                        var delay = FetchRetryDelay * (round - 1);
                        _logger.LogInformation(LogEvents.Network, "Retrying fetch of {FileId} in {Delay} (pass {Round} of {MaxRounds})", fileId, delay, round, MaxFetchRounds);
                        await Task.Delay(delay, ct);
                    }

                    foreach (var node in target.Holders)
                    {
                        if (await TryFetchFromAsync(fileId, node, ct))
                            return;                                  // verified and imported, done

                        // A late transfer from an earlier attempt may have been imported meanwhile
                        if (IsDone(fileId)) return;
                    }
                }

                _logger.LogWarning(LogEvents.Network, "Could not fetch {FileId} from any of {HolderCount} holder(s) after {MaxRounds} passes", fileId, target.Holders.Count, MaxFetchRounds);
            }
            finally { _syncing.TryRemove(fileId, out _); }
        }

        private const int MaxFetchRounds = 3;
        private static readonly TimeSpan FetchRetryDelay = TimeSpan.FromSeconds(10);

        /// <summary>
        /// One attempt to fetch <paramref name="fileId"/> from one holder. Every expected failure (holder not
        /// connected, timeout, failed verification, connection closed) is logged and returns false so the
        /// caller tries the next holder; only cancellation propagates.
        /// </summary>
        private async Task<bool> TryFetchFromAsync(Guid fileId, NodeView node, CancellationToken ct)
        {
            // Look the holder up now rather than using the connection captured when the sync started:
            // by a later pass that connection may have closed and the node reconnected on a new one
            var conn = node.NodeId is { } nodeId ? _peerRegistry.GetByNodeId(nodeId).FirstOrDefault() : null;

            if (conn is null)
            {
                _logger.LogInformation(LogEvents.Network, "Node {NodeId} is not connected; skipping it for {FileId}", node.NodeId, fileId);
                return false;
            }

            var fetch = new InFlightFetch(conn.Context.ConnectionId);
            _inFlight[fileId] = fetch;

            try
            {
                var correlationId = Guid.NewGuid();
                var fileRequest = new FrameBuilder()
                    .Create()
                    .WithType(FrameType.FileRequest)
                    .WithMetadata(new() { CorrelationId = correlationId, NodeId = _nodeIdentity.NodeId })
                    .WithPayload(new Protocol.Messages.Files.FileRequest(fileId, _nodeIdentity.NodeId))
                    .Build();

                _logger.LogInformation(LogEvents.Network, "Requesting file {FileId} from node {NodeId} on connection {ConnectionId}", fileId, node.NodeId, conn.Context.ConnectionId);

                await conn.WriteFrameToClient(fileRequest);

                if (await fetch.Result.Task.WaitAsync(FetchTimeout, ct))
                    return true;

                _logger.LogWarning(LogEvents.Network, "Transfer of {FileId} from node {NodeId} was dropped; trying next holder", fileId, node.NodeId);
            }
            catch (TimeoutException)
            {
                _logger.LogWarning(LogEvents.Network, "Timed out fetching {FileId} from node {NodeId}; trying next holder", fileId, node.NodeId);
            }
            catch (PeerDisconnectedException)
            {
                _logger.LogWarning(LogEvents.Network, "Node {NodeId} disconnected before {FileId} could be requested; trying next holder", node.NodeId, fileId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Not a failure we planned for, so keep the stack trace, but still move on
                _logger.LogWarning(LogEvents.Network, ex, "Failed to request {FileId} from node {NodeId}; trying next holder", fileId, node.NodeId);
            }
            finally { _inFlight.TryRemove(new KeyValuePair<Guid, InFlightFetch>(fileId, fetch)); }

            return false;
        }

        // True once the fetch has nothing left to do: the file is in the ledger and isn't awaiting repair
        private bool IsDone(Guid fileId)
            => !_needsRepair.ContainsKey(fileId)
               && _ledgerService.Get.Items.Any(x => x.ObjectId == fileId);

        private bool HasFile(Guid fileId) => _ledgerService.Get.Items.ToList().Any(x => x.ObjectId == fileId);

        /// <summary>True if <paramref name="connectionId"/> is the connection <paramref name="fileId"/> is currently being fetched from.</summary>
        public bool IsFetchSource(Guid fileId, Guid connectionId)
            => _inFlight.TryGetValue(fileId, out var fetch) && fetch.ConnectionId == connectionId;

        /// <summary>
        /// The checksum a synced file must have, taken from the peers' ledgers rather than the sender's FileEnd.
        /// Null if the file isn't being synced (an ordinary upload).
        /// </summary>
        public string? GetExpectedChecksum(Guid fileId)
            => _syncing.TryGetValue(fileId, out var target) ? target.Expected.Checksum : null;

        /// <summary>
        /// If the verified file is a sync fetch, copies the peers' accepted ledger entry into this node's ledger
        /// and returns true; the caller must not attest it. Returns false for an ordinary upload.
        /// </summary>
        public bool TryCompleteSyncFetch(Guid fileId, Guid connectionId, string checksum)
        {
            if (!_syncing.TryGetValue(fileId, out var target))
                return false;

            if (!string.Equals(target.Expected.Checksum, checksum, StringComparison.OrdinalIgnoreCase))
            {
                // FileHandler already checks against GetExpectedChecksum, so this shouldn't happen
                _logger.LogWarning("Synced file {FileId} has checksum {Actual}, expected {Expected}", fileId, checksum, target.Expected.Checksum);
                FailFetch(fileId, connectionId);
                return true;
            }

            _ledgerService.Add(target.Expected.Checksum, fileId, target.Attestations);
            _needsRepair.TryRemove(fileId, out _);

            _logger.LogInformation("Imported ledger entry for synced file {FileId} with {Count} attestation(s)", fileId, target.Attestations.Count);

            if (_inFlight.TryGetValue(fileId, out var fetch) && fetch.ConnectionId == connectionId)
                fetch.Result.TrySetResult(true);

            return true;
        }

        /// <summary>
        /// Reports a failed transfer so the fetch moves to the next holder. Ignored unless it came from the
        /// connection the file was requested on, so a stray stream from another peer cannot fail a fetch.
        /// </summary>
        public void FailFetch(Guid fileId, Guid connectionId)
        {
            if (_inFlight.TryGetValue(fileId, out var fetch) && fetch.ConnectionId == connectionId)
            {
                fetch.Result.TrySetResult(false);
            }
        }

        public void SetRequestResult(Guid connectionId, Guid correlationId, Protocol.Storage.Ledger ledger)
        {
            // The connection check stops a peer from answering a request that went to someone else
            if (_pendingLedgerRequests.TryGetValue(correlationId, out var pending) && pending.ConnectionId == connectionId)
                pending.Result.TrySetResult((ledger, connectionId));
        }

        private List<SyncTarget> BuildSyncTargets(List<NodeView> nodes, HashSet<Guid> existingFileIds)
        {
            var targets = new List<SyncTarget>();

            var byFile = nodes
                .Where(n => n.NodeId is not null && n.Ledger is not null)
                .SelectMany(n => n.Ledger!.Items.Select(item => (Item: item, Node: n)))
                .Where(x => !existingFileIds.Contains(x.Item.ObjectId))
                .GroupBy(x => x.Item.ObjectId);

            foreach (var file in byFile)
            {
                if (_needsRepair.TryGetValue(file.Key, out var local))
                {
                    var holders = file.Where(x => string.Equals(x.Item.Checksum, local.Checksum, StringComparison.OrdinalIgnoreCase))
                        .Select(x => x.Node)
                        .DistinctBy(n => n.NodeId)
                        .ToList();

                    if (holders.Count == 0)
                    {
                        _logger.LogWarning("No connected peer holdes {FileId} with checksum {Checksum} - will retry next sync", file.Key, local.Checksum);
                        continue;
                    }

                    targets.Add(new SyncTarget(local, local.Attestations, holders));
                    continue;
                }

                var groups = file.GroupBy(x => x.Item.Checksum.ToUpperInvariant()).ToList();

                if (groups.Count != 1)
                {
                    _logger.LogWarning("Skipping sync of {FileId}: holders report {Count} different checksums", file.Key, groups.Count);
                    continue;
                }

                var entries = groups[0].DistinctBy(x => x.Node.NodeId).ToList();

                // Distinct nodes vouching for this checksum: the holders plus the attestations they report.
                // Counting attestations lets a single-peer sync (on HelloResponse) work at all; they are unsigned
                // for now, so this trusts the holders' lists until signed attestations exist.
                var attestations = entries
                    .SelectMany(x => x.Item.Attestations.Append(x.Node.NodeId!.Value))
                    .Distinct()
                    .ToList();

                if (attestations.Count < _attestationOptions.ReplicationFactor)
                {
                    _logger.LogWarning("Skipping sync of {FileId}: {Count} attester(s), need R={R}",
                        file.Key, attestations.Count, _attestationOptions.ReplicationFactor);
                    continue;
                }

                targets.Add(new SyncTarget(entries[0].Item, attestations, entries.Select(x => x.Node).ToList()));
            }

            return targets;
        }
    }

    internal sealed record SyncTarget(LedgerItem Expected, List<Guid> Attestations, List<NodeView> Holders);

    internal sealed record NodeView(Guid? NodeId, Protocol.Storage.Ledger? Ledger, HashSet<Peer> Connections);

    internal sealed class InFlightFetch
    {
        public InFlightFetch(Guid connectionId) => ConnectionId = connectionId;

        public Guid ConnectionId { get; }

        public TaskCompletionSource<bool> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public class LedgerRequest : Request<(Protocol.Storage.Ledger ledger, Guid connectionId)>
    {
        public LedgerRequest(Guid connectionId) => ConnectionId = connectionId;
        public Guid ConnectionId { get; }
    }

    public abstract class Request<T>
    {
        public TaskCompletionSource<T> Result = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
