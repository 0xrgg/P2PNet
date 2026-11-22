using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using PeerNet.Host.Logging;
using PeerNet.Node;
using System.Net;
using System.Net.Sockets;

internal class Program
{
    private static async Task Main(string[] args)
    {vfcbcb
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

        var app = host.Services.GetRequiredService<PeerNetNode>();

        await app.Start();
    }
}
