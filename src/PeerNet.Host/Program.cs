using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using PeerNet.Host.Logging;
using PeerNet.Node;

internal class Program
{
    private static async Task Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        // Levels per namespace come from appsettings.json ("PeerNet": "Debug" etc.)
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole(o => o.FormatterName = PeerNetConsoleFormatter.FormatterName);
        builder.Logging.AddConsoleFormatter<PeerNetConsoleFormatter, ConsoleFormatterOptions>();

        var replicationFactor = builder.Configuration.GetValue<int?>("Attestation:ReplicationFactor");

        builder.Services.AddPeerNet(o =>
        {
            if (replicationFactor is int r)
                o.ReplicationFactor = r;
        });

        using var host = builder.Build();

        // Runs PeerNetNode (a hosted service) until Ctrl+C / SIGTERM, then stops it gracefully
        await host.RunAsync();
    }
}
