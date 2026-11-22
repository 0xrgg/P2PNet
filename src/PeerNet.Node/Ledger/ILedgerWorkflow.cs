using PeerNet.Protocol.Storage;

namespace PeerNet.Node.Ledger
{
    public interface ILedgerWorkflow
    {
        Task RequestLedgersFromAvailableNodes();
        Task RequestLedgerFromAvailableNode(Guid nodeId);
        void SetRequestResult(Guid connectionId, Guid correlationId, Protocol.Storage.Ledger ledger);
        void FailFetch(Guid fileId, Guid connectionId);
        bool IsFetchSource(Guid fileId, Guid connectionId);
        string? GetExpectedChecksum(Guid fileId);
        bool TryCompleteSyncFetch(Guid fileId, Guid connectionId, string checksum);
        void MarkForRepair(LedgerItem item);
    }
}