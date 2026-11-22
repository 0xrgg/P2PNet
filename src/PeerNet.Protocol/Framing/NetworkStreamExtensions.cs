using System.Buffers.Binary;
using System.Net.Sockets;
using System.Text.Json;

namespace PeerNet.Protocol.Framing;

public static class NetworkStreamExtensions
{
    public static async Task<byte[]> ReadExactAsync(
        this NetworkStream stream,
        int count,
        CancellationToken ct)
    {
        var buffer = new byte[count];
        var offset = 0;

        while (offset < count)
        {
            var bytesRead = await stream.ReadAsync(
                buffer,
                offset,
                count - offset,
                ct);

            if (bytesRead == 0)
                throw new EndOfStreamException(
                    "Remote closed the connection.");

            offset += bytesRead;
        }

        return buffer;
    }


    public static async Task<PeerNetMessage> ReceiveFrameAsync(
        this NetworkStream stream,
        CancellationToken ct)
    {
        // 1. Read 4-byte length prefix
        byte[] lengthBytes =
            await stream.ReadExactAsync(4, ct);

        int totalLength =
            BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);

        // Minimum: type (2) + metadata length (2) + Guid (16)
        const int minimumFrameLength = 20;

        if (totalLength < minimumFrameLength)
        {
            throw new InvalidDataException(
                $"Invalid frame length: {totalLength}");
        }

        // 2. Read the complete frame
        byte[] frame =
            await stream.ReadExactAsync(totalLength, ct);

        // 3. First 2 bytes are the message type
        ushort rawType =
            BinaryPrimitives.ReadUInt16LittleEndian(
                frame.AsSpan(0, 2));

        var type = (FrameType)rawType;

        // 4. Next 2 bytes are the metadata length
        ushort metadataLength =
            BinaryPrimitives.ReadUInt16LittleEndian(
                frame.AsSpan(2, 2));

        // 5. Validate metadata length
        int payloadOffset = 4 + metadataLength;
        int guidOffset = totalLength - 16;

        if (payloadOffset > guidOffset)
        {
            throw new InvalidDataException(
                "Metadata length exceeds available frame data.");
        }

        // 6. Deserialize metadata
        var metadataBytes =
            frame.AsSpan(4, metadataLength).ToArray();

        var metadata = new FrameMetadata();

        if (metadataBytes.Length > 0)
        {
            metadata =
                JsonSerializer.Deserialize<FrameMetadata>(metadataBytes)
                ?? throw new InvalidDataException("Invalid frame metadata");
        }

        // 7. Extract payload (excluding final 16 bytes)
        byte[] payload =
            frame.AsSpan(payloadOffset, guidOffset - payloadOffset).ToArray();

        // 8. Extract FrameId
        Guid frameId =
            new Guid(frame.AsSpan(guidOffset, 16));

        return new PeerNetMessage(type, metadata, payload, frameId);
    }
}
