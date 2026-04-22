namespace BackupsterAgent.Services.Common.Progress;

public sealed class NullProgressReporter<TStage> : IProgressReporter<TStage>
    where TStage : struct, Enum
{
    public void Report(
        TStage stage,
        long? processed = null,
        long? total = null,
        string? unit = null,
        string? currentItem = null)
    {
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
