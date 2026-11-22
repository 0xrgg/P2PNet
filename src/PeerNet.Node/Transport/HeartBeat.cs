namespace PeerNet.Node.Transport;

public class HeartBeat
{
    public TaskCompletionSource<bool>? _pending;

    public TaskCompletionSource<bool> StartPing()
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Interlocked.CompareExchange(ref _pending, tcs, null);

        return _pending;
    }

    public void PongReceived()
    {
        Interlocked.Exchange(ref _pending, null)?.TrySetResult(true);
    }

    public void Cancel(TaskCompletionSource<bool> tcs)
    {
        Interlocked.CompareExchange(ref _pending, null, tcs);
    }
}
