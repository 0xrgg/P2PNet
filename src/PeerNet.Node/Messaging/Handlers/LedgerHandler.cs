using Microsoft.Extensions.Logging;
using PeerNet.Node.Files;
using PeerNet.Node.Ledger;
using PeerNet.Node.Peers;
using PeerNet.Node.Storage;
using PeerNet.Protocol.Framing;
using PeerNet.Protocol.Messages.Files;
using PeerNet.Protocol.Messages.Ledger;

namespace PeerNet.Node.Messaging.Handlers;

/// <summary>LedgerRequest / LedgerResponse / FileRequest: the request side of ledger sync and the serving side for peers.</summary>
public sealed class LedgerHandler : IMessageHandler
{
    private readonly ILedgerService _ledgerService;
    private readonly ILedgerWorkflow _ledgerWorkflow;
    private readonly IFileWorkflow _fileWorkflow;
    private readonly IPeerRegistry _registry;
    private readonly ILogger<LedgerHandler> _logger;

    public LedgerHandler(ILedgerService ledgerService, ILedgerWorkflow ledgerWorkflow, IFileWorkflow fileWorkflow, IPeerRegistry registry, ILogger<LedgerHandler> logger)
    {
        _ledgerService = ledgerService;
        _ledgerWorkflow = ledgerWorkflow;
        _fileWorkflow = fileWorkflow;
        _registry = registry;
        _logger = logger;
    }

    public IReadOnlyCollection<FrameType> Handles => [FrameType.LedgerRequest, FrameType.LedgerResponse, FrameType.FileRequest];

    public async Task HandleAsync(InboundMessage msg, CancellationToken ct)
    {
        switch (msg.Type)
        {
            case FrameType.LedgerResponse:
                _ledgerWorkflow.SetRequestResult(msg.ConnectionId, msg.Metadata.CorrelationId, ((LedgerResponse)msg.Payload!).Ledger);
                return;

            case FrameType.LedgerRequest:
            {
                // Reply only on the connection the request came in on
                var peer = _registry.Get(msg.ConnectionId);
                if (peer is null) return;

                var response = new FrameBuilder()
                    .Create()
                    .WithType(FrameType.LedgerResponse)
                    .WithPayload(_ledgerService.Get)
                    .WithMetadata(new() { CorrelationId = msg.Metadata.CorrelationId })
                    .Build();

                await peer.WriteFrameToClient(response);
                return;
            }

            case FrameType.FileRequest:
            {
                var req = (FileRequest)msg.Payload!;
                _logger.LogInformation("{NodeId} requested file {FileId} on connection {ConnectionId}", req.RequestingNodeId, req.FileId, msg.ConnectionId);

                // Reply only on the connection the request came in on, never by the NodeId in the payload
                var peer = _registry.Get(msg.ConnectionId);
                if (peer is null) return;

                await _fileWorkflow.ServeFile(req.FileId, peer);
                return;
            }
        }
    }
}
