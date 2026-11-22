using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Console;

namespace PeerNet.Host.Logging;

/// <summary>
/// Single-line console formatter that colours the whole line by kind of event
/// (see PeerNet.Node.Logging.LogEvents), with warnings and errors always taking precedence.
/// Colour is turned off when output is redirected, so piped or remote logs stay clean.
/// </summary>
public sealed class PeerNetConsoleFormatter : ConsoleFormatter
{
    public const string FormatterName = "peernet";

    private const string Reset = "\u001b[0m";
    private const string Red = "\u001b[31m";
    private const string BrightRed = "\u001b[91m";
    private const string Yellow = "\u001b[33m";
    private const string Grey = "\u001b[90m";
    private const string Cyan = "\u001b[36m";
    private const string Green = "\u001b[32m";
    private const string Magenta = "\u001b[35m";

    private readonly bool _useColor = !Console.IsOutputRedirected;

    public PeerNetConsoleFormatter() : base(FormatterName)
    {
    }

    public override void Write<TState>(
        in LogEntry<TState> logEntry,
        IExternalScopeProvider? scopeProvider,
        TextWriter textWriter)
    {
        var message = logEntry.Formatter(logEntry.State, logEntry.Exception);

        if (string.IsNullOrEmpty(message) && logEntry.Exception is null)
            return;

        var color = _useColor ? ColorFor(logEntry.LogLevel, logEntry.EventId) : null;

        if (color is not null)
            textWriter.Write(color);

        textWriter.Write(
            $"{DateTime.Now:HH:mm:ss.fff} {LevelLabel(logEntry.LogLevel)} [{TagFor(logEntry)}] {message}");

        if (logEntry.Exception is not null)
        {
            textWriter.WriteLine();
            textWriter.Write(logEntry.Exception);
        }

        if (color is not null)
            textWriter.Write(Reset);

        textWriter.WriteLine();
    }

    private static string? ColorFor(LogLevel level, EventId eventId) => level switch
    {
        LogLevel.Critical => BrightRed,
        LogLevel.Error => Red,
        LogLevel.Warning => Yellow,
        LogLevel.Debug or LogLevel.Trace => Grey,
        _ => (eventId.Id / 1000) switch
        {
            1 => Cyan,      // LogEvents.Setup
            2 => Green,     // LogEvents.Network
            3 => Magenta,   // LogEvents.Consensus
            _ => null,      // framework and uncategorised: terminal default
        }
    };

    // Event name when there is one, otherwise the last segment of the category (e.g. "Lifetime")
    private static string TagFor<TState>(in LogEntry<TState> logEntry) =>
        logEntry.EventId.Name
        ?? logEntry.Category[(logEntry.Category.LastIndexOf('.') + 1)..];

    private static string LevelLabel(LogLevel level) => level switch
    {
        LogLevel.Trace => "trce",
        LogLevel.Debug => "dbug",
        LogLevel.Information => "info",
        LogLevel.Warning => "warn",
        LogLevel.Error => "fail",
        LogLevel.Critical => "crit",
        _ => "    ",
    };
}
