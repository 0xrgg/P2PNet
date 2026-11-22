using Microsoft.Extensions.Logging;
using PeerNet.Node.Storage;
using PeerNet.Protocol.Storage;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;


namespace PeerNet.Node.Files
{
    public class FileIntegrityChecker
    {
        private readonly ILedgerService _ledgerService;
        private readonly ILogger<FileIntegrityChecker> _logger;

        public FileIntegrityChecker(ILedgerService ledgerService, ILogger<FileIntegrityChecker> logger)
        {
            _ledgerService = ledgerService;
            _logger = logger;
        }

        public async IAsyncEnumerable<FileIntegrityIssue> VerifyFileIntegrity([EnumeratorCancellation] CancellationToken ct = default)
        {
            var items = _ledgerService.Snapshot();

            foreach (var item in items)
            {
                ct.ThrowIfCancellationRequested();

                _logger.LogDebug("Validating file {FileId} from ledger item {ItemId}", item.ObjectId, item.Id);

                var zipPath = Path.Combine(NodePaths.ReceivedFiles, $"{item.ObjectId}.zip");

                FileIssue? issue = null;

                try
                {

                    var hash = await HashZipContentsAsync(zipPath, ct);
                    if (!string.Equals(item.Checksum, hash, StringComparison.OrdinalIgnoreCase))
                    {
                        issue = FileIssue.Mismatch;
                    }
                }
                catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException) { issue = FileIssue.Missing; }
                catch (InvalidDataException) { issue = FileIssue.Corrupt; }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    // Can't read it right now; skip it rather than end the whole check
                    _logger.LogWarning("Could not check {FileId}: {Reason}", item.ObjectId, ex.Message);
                    continue;
                }

                if (issue is { } i) yield return new(item, i);
                else _logger.LogDebug("File {FileId} verified", item.ObjectId);
            }
        }

        private static async Task<string> HashZipContentsAsync(
            string zipPath,
            CancellationToken ct = default)
        {
            await using var fileStream = File.OpenRead(zipPath);
            using var archive = new ZipArchive(fileStream, ZipArchiveMode.Read);

            if (archive.Entries.Count != 1)
                throw new InvalidDataException(
                    $"Expected exactly one file in ZIP, found {archive.Entries.Count}.");

            var entry = archive.Entries[0];

            await using var entryStream = entry.Open();

            byte[] hash = await SHA256.HashDataAsync(entryStream, ct);

            return Convert.ToHexString(hash);
        }
    }

    public enum FileIssue { Missing, Corrupt, Mismatch }
    public readonly record struct FileIntegrityIssue(LedgerItem Item, FileIssue Issue);
}
