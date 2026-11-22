using System.Buffers.Binary;
using System.Text;
using System.Text.Json;

namespace PeerNet.Protocol.Framing;

public static class FrameSerializer
{
    public static byte[] Serialize(PeerNetFrame frame)
    {
        // Serialize payload
        byte[] framePayload = frame.Payload switch
        {
            null => [],
            byte[] bytes => bytes,
            string str => Encoding.UTF8.GetBytes(str),
            var payload => JsonSerializer.SerializeToUtf8Bytes(
                payload,
                payload.GetType())
        };

        // Serialize metadata
        byte[] metadataPayload = frame.Metadata is not null
            ? JsonSerializer.SerializeToUtf8Bytes(frame.Metadata)
            : [];

        // Calculate frame length (excluding 4-byte length prefix)
        int frameLength =
            sizeof(ushort) +          // Frame type
            sizeof(ushort) +          // Metadata length
            metadataPayload.Length +
            framePayload.Length +
            16;                       // GUID

        using var stream = new MemoryStream();

        // 1. Frame length (4 bytes)
        Span<byte> lengthBuffer = stackalloc byte[4];

        BinaryPrimitives.WriteInt32LittleEndian(
            lengthBuffer,
            frameLength);

        stream.Write(lengthBuffer);

        // 2. Frame type (2 bytes)
        Span<byte> typeBuffer = stackalloc byte[2];

        BinaryPrimitives.WriteUInt16LittleEndian(
            typeBuffer,
            (ushort)frame.FrameType);

        stream.Write(typeBuffer);

        // 3. Metadata length (2 bytes)
        Span<byte> metadataLengthBuffer = stackalloc byte[2];

        BinaryPrimitives.WriteUInt16LittleEndian(
            metadataLengthBuffer,
            (ushort)metadataPayload.Length);

        stream.Write(metadataLengthBuffer);

        // 4. Metadata
        stream.Write(metadataPayload);

        // 5. Payload
        stream.Write(framePayload);

        // 6. Frame ID (16 bytes)
        stream.Write(frame.Id.ToByteArray());

        return stream.ToArray();
    }
}
