namespace PeerNet.Protocol.Messages.Files;

// TODO: decouple this from envelope - currently this is easier than creating an intermediary object and mapping between
public class FileReceivedNotification : IEnvelope
{
    public Guid FileTransferId { get; set; }
    public Guid CorrelationId { get; set; }
    public string Checksum { get; set; }
    public string Path { get; set; }
}
