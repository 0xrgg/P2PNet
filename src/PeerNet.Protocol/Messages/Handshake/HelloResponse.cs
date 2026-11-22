namespace PeerNet.Protocol.Messages.Handshake;

public sealed record HelloResponse(Guid NodeId) : IEnvelope;
