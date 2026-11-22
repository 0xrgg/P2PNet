namespace PeerNet.Node;

/// <summary>
/// On-disk locations used by the node, all relative to the application's base directory.
/// </summary>
public static class NodePaths
{
    public static readonly string AppData = Path.Combine(AppContext.BaseDirectory, "AppData");
    public static readonly string ReceivedFiles = Path.Combine(AppContext.BaseDirectory, "ReceivedFiles");

    public static readonly string Ledger = Path.Combine(AppData, "ledger.json");

    public static readonly string Keys = Path.Combine(AppData, "Keys");
    public static readonly string PublicKey = Path.Combine(Keys, "node_public_key");
    public static readonly string PrivateKey = Path.Combine(Keys, "node_private_key");

    public static readonly string DropZone = Path.Combine(AppContext.BaseDirectory, "DropZone");
    public static readonly string DropZonePublished = Path.Combine(DropZone, "Published");
    public static readonly string DropZoneFailed = Path.Combine(DropZone, "Failed");

}
