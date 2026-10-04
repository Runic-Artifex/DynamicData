namespace DynamicData.Tests.Cache;

public static partial class ExpireAfterFixture
{
    public sealed class DisposalRace
    {
    [Test]
    public async Task DisposalDuringScheduledEdit_DoesNotEscapeOntoScheduler()
    {
        using var inner = new SourceCache<TestItem, int>(item => item.Id);
        using var source = new BlockedEditCache(inner);
        var scheduler = CreateTestScheduler();
        using var subscription = source.ExpireAfter(_ => TimeSpan.FromMilliseconds(10), scheduler: scheduler)
            .RecordValues(out var results, scheduler);
        inner.AddOrUpdate(new TestItem { Id = 1 });
        source.Block = true;
        var advance = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        new Thread(() =>
        {
            try { scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks); advance.TrySetResult(); }
            catch (Exception error) { advance.TrySetException(error); }
        }) { IsBackground = true }.Start();
        try
        {
            await source.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            subscription.Dispose();
            inner.Dispose();
        }
        finally { source.Release.Set(); await advance.Task.WaitAsync(TimeSpan.FromSeconds(30)); }
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues).IsEmpty();
    }
    }

    private sealed class BlockedEditCache(SourceCache<TestItem, int> inner) : ISourceCache<TestItem, int>
    {
        internal bool Block { get; set; }
        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal ManualResetEventSlim Release { get; } = new();
        public int Count => inner.Count;
        public IReadOnlyList<TestItem> Items => inner.Items;
        public IReadOnlyList<int> Keys => inner.Keys;
        public IReadOnlyDictionary<int, TestItem> KeyValues => inner.KeyValues;
        public Func<TestItem, int> KeySelector => inner.KeySelector;
        public IObservable<int> CountChanged => inner.CountChanged;
        public ReactiveUI.Primitives.Optional<TestItem> Lookup(int key) => inner.Lookup(key);
        public IObservable<Change<TestItem, int>> Watch(int key) => inner.Watch(key);
        public IObservable<IChangeSet<TestItem, int>> Connect(Func<TestItem, bool>? predicate = null, bool suppressEmptyChangeSets = true) => inner.Connect(predicate, suppressEmptyChangeSets);
        public IObservable<IChangeSet<TestItem, int>> Preview(Func<TestItem, bool>? predicate = null) => inner.Preview(predicate);
        public void Edit(Action<ISourceUpdater<TestItem, int>> updateAction)
        {
            if (Block)
            {
                Entered.TrySetResult();
                Release.Wait();
                throw new ObjectDisposedException(nameof(SourceCache<TestItem, int>));
            }
            inner.Edit(updateAction);
        }
        public void Dispose() { Release.Set(); inner.Dispose(); Release.Dispose(); }
    }
}
