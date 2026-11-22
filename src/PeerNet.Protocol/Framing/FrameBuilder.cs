namespace PeerNet.Protocol.Framing;

public class FrameBuilder
{
    public PeerNetFrame _frame;

    public FrameBuilder Create() 
    {
        _frame = new();
        return this;
    }

    public FrameBuilder WithMetadata(FrameMetadata metadata)
    {
        _frame.Metadata = metadata;
        return this;
    }

    public FrameBuilder WithType(FrameType frameType)
    {
        _frame.FrameType = frameType;
        return this;
    }

    public FrameBuilder WithPayload(object payload)
    {
        _frame.Payload = payload;
        return this;
    }

    public PeerNetFrame Build() => _frame;
}
