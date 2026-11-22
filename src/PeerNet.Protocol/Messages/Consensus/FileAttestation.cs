using System.Security.Cryptography.X509Certificates;

namespace PeerNet.Protocol.Messages.Consensus;

public sealed record FileAttestation(Guid FileId, string Checksum, Guid NodeId, byte[] PublicKey, byte[] Payload) : IEnvelope;