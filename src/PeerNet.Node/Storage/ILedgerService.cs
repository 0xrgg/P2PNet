
using PeerNet.Protocol.Storage;

namespace PeerNet.Node.Storage
{
    public interface ILedgerService
    {
        Protocol.Storage.Ledger Get { get; }

        List<LedgerItem> Snapshot();

        LedgerItem Add(string checksum, Guid objectId, IEnumerable<Guid> attestations);
        Task BuildLedgerFromJson();
        LedgerItem GetByObjectId(Guid objectId);
        void PersistToDisk();

        event Action<LedgerItem>? ItemAdded;
    }
}