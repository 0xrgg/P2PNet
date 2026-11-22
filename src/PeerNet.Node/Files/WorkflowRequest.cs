namespace PeerNet.Node.Files;

public class WorkflowRequest
{
    private TaskCompletionSource<bool>? _pendingRequest;

    public TaskCompletionSource<bool> BeginTransfer()
    {
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        Interlocked.CompareExchange(ref _pendingRequest, tcs, null);

        return _pendingRequest;
    }

    public void ConfirmationReceived()
    {
        Interlocked.Exchange(ref _pendingRequest, null)?.TrySetResult(true);
    }

    public void Cancel(TaskCompletionSource<bool> tcs)
    {
        Interlocked.CompareExchange(ref _pendingRequest, null, tcs);
    }
}
