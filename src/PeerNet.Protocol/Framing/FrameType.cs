namespace PeerNet.Protocol.Framing;

public enum FrameType
{
    // Reserved for sys
    Hello = 0,
    HelloResponse = 1,
    Ping = 2,
    Pong = 3,
    // Reserved for files
    FileStart = 10,
    FileChunk = 11,
    FileEnd = 12,
    FileRequest = 15,
    // Reserved for consensus mechanism
    Attestation = 24,
    LedgerRequest = 25,
    LedgerResponse = 26

    // Retired, never reuse: 13 FileReceived, 14 ClientReceived, 20 ValidationRequest,
    // 21 ConsensusRequest, 22 ConsensusVote, 23 PropagateConsensus.
}
