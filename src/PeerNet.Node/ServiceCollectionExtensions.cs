using Microsoft.Extensions.DependencyInjection;
using PeerNet.Node.Consensus;
using PeerNet.Node.Discovery;
using PeerNet.Node.Files;
using PeerNet.Node.Ledger;
using PeerNet.Node.Messaging;
using PeerNet.Node.Messaging.Handlers;
using PeerNet.Node.Peers;
using PeerNet.Node.Storage;
using PeerNet.Node.Transport;
using PeerNet.Protocol.Codecs;

namespace PeerNet.Node;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddPeerNet(
        this IServiceCollection services,
        Action<AttestationOptions>? configureAttestation = null)
    {
        // Persisted key, so the NodeId survives restarts; resolved before anything can read it
        services.AddSingleton(NodeIdentity.LoadOrCreate(NodePaths.PublicKey, NodePaths.PrivateKey));
        services.AddSingleton<PeerNetNode>();

        // Discovery
        services.AddSingleton<IPeerNetBootstrap, PeerNetBootstrap>();

        // Storage
        services.AddSingleton<ILedgerService, LedgerService>();

        // Transport
        services.AddSingleton<IPeerFactory, PeerFactory>();
        services.AddSingleton<IPeerConnector, PeerConnector>();
        services.AddSingleton<ITcpServer, TcpServer>();
        services.AddSingleton<IPeerRegistry, PeerRegistry>();

        // Protocol
        services.AddSingleton<IDecoder, Decoder>();
        services.AddSingleton<ICodec, PingCodec>();
        services.AddSingleton<ICodec, PongCodec>();
        services.AddSingleton<ICodec, HelloCodec>();
        services.AddSingleton<ICodec, HelloResponseCodec>();
        services.AddSingleton<ICodec, FileStartCodec>();
        services.AddSingleton<ICodec, FileEndCodec>();
        services.AddSingleton<ICodec, FileChunkCodec>();
        services.AddSingleton<ICodec, FileAttestationCodec>();
        services.AddSingleton<ICodec, LedgerResponseCodec>();
        services.AddSingleton<ICodec, LedgerRequestCodec>();
        services.AddSingleton<ICodec, FileRequestCodec>();

        // Features
        services.AddSingleton<IEventBus, EventBus>();
        services.AddSingleton<FileHandler>();
        services.AddSingleton<IFileWorkflow, FileWorkflow>();
        services.AddSingleton<ILedgerWorkflow, LedgerWorkflow>();

        // Consensus
        var attestationOptions = new AttestationOptions();
        configureAttestation?.Invoke(attestationOptions);
        services.AddSingleton(attestationOptions);
        services.AddSingleton<IAttestationTracker, AttestationTracker>();

        // Inbound message handlers, routed by FrameType
        services.AddSingleton<IMessageHandler, HandshakeHandler>();
        services.AddSingleton<IMessageHandler, FileTransferHandler>();
        services.AddSingleton<IMessageHandler, LedgerHandler>();
        services.AddSingleton<IMessageHandler, AttestationHandler>();
        services.AddSingleton<EventDispatcher>();

        services.AddSingleton<FileIntegrityChecker>();

        services.AddSingleton<Outbox>();

        return services;
    }
}
