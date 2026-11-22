using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PeerNet.Node.Discovery;
using PeerNet.Node.Files;
using PeerNet.Node.Ledger;
using PeerNet.Node.Messaging;
using PeerNet.Node.Storage;
using PeerNet.Node.Transport;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace PeerNet.Node;

/// <summary>
/// Runs the node for the lifetime of the host: starts the TCP server, consumes inbound messages,
/// discovers peers, verifies ledger files and drains the outbox. The host cancels <c>stoppingToken</c> on shutdown (e.g. Ctrl+C).
/// </summary>
public class PeerNetNode : BackgroundService
{
    private readonly ITcpServer _peerNetServer;
    private readonly IEventBus _eventBus;
    private readonly IPeerNetBootstrap _bootstrap;
    private readonly ILedgerService _ledgerService;
    private readonly ILogger<PeerNetNode> _logger;
    private readonly EventDispatcher _dispatcher;
    private readonly NodeIdentity _identity;
    private readonly FileIntegrityChecker _fileChecker;
    private readonly Outbox _outbox;
    private readonly ILedgerWorkflow _ledgerWorkflow;

    public IPAddress HostAddress { get; set; }

    public Guid NodeId => _identity.NodeId;

    public PeerNetNode(
        NodeIdentity identity,
        ITcpServer peerNetServer,
        IEventBus eventBus,
        EventDispatcher dispatcher,
        IPeerNetBootstrap bootstrap,
        ILedgerService ledgerService,
        ILogger<PeerNetNode> logger,
        ILedgerWorkflow ledgerWorkflow,
        FileIntegrityChecker fileChecker,
        Outbox outbox)
    {
        _peerNetServer = peerNetServer;
        _eventBus = eventBus;
        _dispatcher = dispatcher;
        _identity = identity;
        _bootstrap = bootstrap;
        _ledgerService = ledgerService;
        _logger = logger;
        _ledgerWorkflow = ledgerWorkflow;
        _fileChecker = fileChecker;
        _outbox = outbox;

        CalculateIpAddress();
    }

    private void CalculateIpAddress()
    {
        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (nic.OperationalStatus != OperationalStatus.Up ||
                nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                continue;

            foreach (var unicast in nic.GetIPProperties()
                .UnicastAddresses
                .Where(x => x.PrefixLength == 24))
            {
                if (unicast.Address.AddressFamily != AddressFamily.InterNetwork)
                    continue;

                HostAddress = unicast.Address;
            }
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Linked so that if one long-running loop ends, the other is stopped too
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var ct = cts.Token;

        _logger.LogInformation("Starting node with ID: {NodeId}", NodeId);

        await _ledgerService.BuildLedgerFromJson();

        // 1. Start local TCP server in background
        StartServer();

        // 2. Consume inbound frames before discovery starts dialling peers
        var eventLoop = RunEventLoopAsync(ct);

        // 3. Start discovery broadcast (runs 5 times and finishes, listener stays active)
        await _bootstrap.StartDiscovery(HostAddress, NodeId, ct);

        // 4. One-shot integrity pass; off the startup path and logs its own errors
        _ = Task.Run(() => VerifyLedgerFiles(ct), ct);

        // 5. Publish files dropped into the DropZone
        var outbox = _outbox.RunAsync(ct);

        // The node lives as long as its long-running loops do. If either one ends or faults,
        // stop the other and rethrow, so the host logs the failure and shuts down instead of hanging.
        var finished = await Task.WhenAny(eventLoop, outbox);
        await cts.CancelAsync();
        await finished;
    }

    private async Task RunEventLoopAsync(CancellationToken cancellationToken)
    {
        // Single consumer: handlers run one at a time, in arrival order
        await foreach (var msg in _eventBus.ReadAllAsync(cancellationToken))
        {
            await _dispatcher.DispatchAsync(msg, cancellationToken);
        }
    }

    private async Task VerifyLedgerFiles(CancellationToken ct)
    {
        try
        {
            var found = 0;

            await foreach (var issue in _fileChecker.VerifyFileIntegrity(ct))
            {
                _logger.LogError("An issue was found with ledger item: {Id}", issue.Item.ObjectId);
                _ledgerWorkflow.MarkForRepair(issue.Item);
                found++;
            }

            if (found > 0)
            {
                await _ledgerWorkflow.RequestLedgersFromAvailableNodes();
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutting down
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred checking file integrity");
        }
    }

    private void StartServer()
    {
        _peerNetServer.StartServer(HostAddress);
    }
}
