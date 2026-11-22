using Microsoft.Extensions.Logging;

namespace PeerNet.Node.Logging;

/// <summary>
/// Event IDs that classify log lines by concern. The thousands digit is the kind
/// (1xxx setup, 2xxx network, 3xxx consensus), which the console formatter uses for colour,
/// so keep any new IDs inside their kind's range.
/// </summary>
public static class LogEvents
{
    /// <summary>Configuration and local resources: storage, listeners, startup.</summary>
    public static readonly EventId Setup = new(1000, nameof(Setup));

    /// <summary>Connections, frames, discovery and file transmission.</summary>
    public static readonly EventId Network = new(2000, nameof(Network));

    /// <summary>Consensus rounds, votes and outcomes.</summary>
    public static readonly EventId Consensus = new(3000, nameof(Consensus));
}
