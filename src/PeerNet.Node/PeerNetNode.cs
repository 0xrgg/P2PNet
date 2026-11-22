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

public class PeerNetNode
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

    private readonly CancellationTokenSource _cts = new();

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

    private async Task RunEventLoopAsync(CancellationToken cancellationToken)
    {
        // Single consumer: handlers run one at a time, in arrival order
        await foreach (var msg in _eventBus.ReadAllAsync(cancellationToken))
        {
            await _dispatcher.DispatchAsync(msg, cancellationToken);
        }
    }

    public async Task Start()
    {
        _logger.LogInformation("Starting node with ID: {NodeId}", _identity.NodeId);

        await _ledgerService.BuildLedgerFromJson();

        // 1. Start local TCP server in background
        StartServer();

        // 2. Consume inbound frames before discovery starts dialling peers
        _ = RunEventLoopAsync(_cts.Token);

        // 3. Start discovery broadcast (runs 5 times and finishes, listener stays active)
        await _bootstrap.StartDiscovery(HostAddress, NodeId, _cts.Token);

        _ = Task.Run(async () =>
        {
            try
            {
                var found = 0;

                await foreach (var issue in _fileChecker.VerifyFileIntegrity(_cts.Token))
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
            catch (Exception ex)
            {
                _logger.LogError("An error occurred checking file integrity: {ExMessage}", ex.Message);
            }

        });

        _ = _outbox.RunAsync(_cts.Token);

        // Keep the application running indefinitely
        await Task.Delay(Timeout.Infinite, _cts.Token);
    }

    public void StartServer()
    {
        _peerNetServer.StartServer(HostAddress);
    }


}
