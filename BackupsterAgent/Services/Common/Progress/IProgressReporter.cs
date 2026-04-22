namespace BackupsterAgent.Services.Common.Progress;

public interface IProgressReporter<TStage> : IAsyncDisposable
    where TStage : struct, Enum
{
    void Report(
        TStage stage,
        long? processed = null,
        long? total = null,
        string? unit = null,
        string? currentItem = null);
}
