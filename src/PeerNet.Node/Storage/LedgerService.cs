using Microsoft.Extensions.Logging;
using PeerNet.Node.Consensus;
using PeerNet.Node.Logging;
using PeerNet.Protocol.Storage;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace PeerNet.Node.Storage
{
    public class LedgerService : ILedgerService
    {
        private object _lock { get; set; } = new();

        public const string GenesisHash = "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

        private Protocol.Storage.Ledger Ledger { get; set; } = new();

        private readonly ILogger<LedgerService> _logger;

        private static readonly string DataPath = NodePaths.AppData;
        private static readonly string LedgerPath = NodePaths.Ledger;

        public Protocol.Storage.Ledger Get => Ledger;

        public LedgerItem GetByObjectId(Guid objectId) => Ledger.Items.First(x => x.ObjectId == objectId);

        public int LedgerCount => Ledger.Items.Count();

        public event Action<LedgerItem>? ItemAdded;

        public LedgerService(ILogger<LedgerService> logger)
        {
            _logger = logger;

            CreateApplicationDependencies();
        }

        public List<LedgerItem> Snapshot()
        {
            lock (_lock)
            {
                return Ledger.Items.ToList();
            }
        }

        private static string ComputeEntryHash(string previousHash, Guid objectId, string checksum)
        {
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA512);
            hasher.AppendData(Encoding.UTF8.GetBytes(previousHash));
            hasher.AppendData(Encoding.UTF8.GetBytes(objectId.ToString()));
            hasher.AppendData(Encoding.UTF8.GetBytes(checksum));
            return Convert.ToHexString(hasher.GetHashAndReset());
        }

        public async Task BuildLedgerFromJson()
        {

            _logger.LogInformation(LogEvents.Setup, "Building ledger from {Path}", LedgerPath);
            var ledgerContent = await File.ReadAllTextAsync(LedgerPath);

            if (string.IsNullOrWhiteSpace(ledgerContent))
            {
                return;
            }

            Ledger = JsonSerializer.Deserialize<Protocol.Storage.Ledger>(ledgerContent) ?? throw new InvalidDataException("Ledger could not be parsed - the file may be corrupted");

            var result = VerifyChain();

            _logger.LogInformation(LogEvents.Setup, "Restored ledger with {Count} entries", Ledger.Items.Count);
            _logger.LogDebug(LogEvents.Setup, "Ledger contents: {Ledger}", JsonSerializer.Serialize(Ledger));
        }

        public int? VerifyChain()
        {
            lock (_lock)
            {
                var expectedPrevious = GenesisHash;

                for (var i = 0; i < Ledger.Items.Count; i++)
                {
                    var item = Ledger.Items[i];

                    if (item.PreviousHash != expectedPrevious)
                        return i;   // broken link: entry removed, inserted or reordered

                    if (ComputeEntryHash(item.PreviousHash, item.ObjectId, item.Checksum) != item.EntryHash)
                        return i;   // contents changed after the hash was written

                    expectedPrevious = item.EntryHash;
                }
                return null;
            }
        }

        public void PersistToDisk()
        {
            lock (_lock)
            {
                var json = JsonSerializer.Serialize(Ledger);
                File.WriteAllText(LedgerPath, json);
            }
        }

        private void CreateApplicationDependencies()
        {
            lock (_lock)
            {
                if (!Directory.Exists(DataPath))
                {
                    _logger.LogInformation(LogEvents.Setup, "Creating directory {Path}", DataPath);
                    Directory.CreateDirectory(DataPath);
                }

                if (!File.Exists(LedgerPath))
                {
                    _logger.LogInformation(LogEvents.Setup, "Creating ledger file {Path}", LedgerPath);
                    File.WriteAllText(LedgerPath, JsonSerializer.Serialize(new Protocol.Storage.Ledger()));
                }
            }
        }

        public LedgerItem Add(string checksum, Guid objectId, IEnumerable<Guid> attestations)
        {
            LedgerItem added;

            lock (_lock)
            {
                var ledgerItems = Snapshot();

                if (ledgerItems.FirstOrDefault(x => x.ObjectId == objectId) is { } item)
                {
                    return item;
                }

                var previousHash = ledgerItems.Count > 0
                    ? Ledger.Items[^1].EntryHash 
                    : GenesisHash;

                var entryHash = ComputeEntryHash(previousHash, objectId, checksum);

                added = new LedgerItem
                {
                    Checksum = checksum,
                    ObjectId = objectId,
                    PreviousHash = previousHash,
                    EntryHash = entryHash,
                    Attestations = attestations.ToList()
                };

                PersistToDisk();
                Ledger.Items.Add(added);
            }

            ItemAdded?.Invoke(added);

            return added;
        }
    }
}
