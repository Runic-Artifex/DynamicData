namespace DynamicData.Tests.List;

public sealed class ExpireAfterOccurrenceFixture
{
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public Task EqualReferences_QueuedMovePreservesOccurrenceDeadlines(bool polling, bool blockSentinel)
        => VerifyQueuedMove(new EqualReference(1), new EqualReference(2), new EqualReference(3),
            static (left, right) => ReferenceEquals(left, right), polling, blockSentinel);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public Task SameReference_QueuedMovePreservesOccurrenceDeadlines(bool polling)
    {
        var repeated = new EqualReference(1);
        return VerifyQueuedMove(repeated, repeated, new EqualReference(3),
            static (left, right) => ReferenceEquals(left, right), polling, false);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public Task EqualValues_QueuedMovePreservesOccurrenceDeadlines(bool polling)
        => VerifyQueuedMove(new EqualValue(1), new EqualValue(2), new EqualValue(3),
            static (left, right) => left.Id == right.Id, polling, false);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public Task IdenticalValues_QueuedMovePreservesOccurrenceDeadlines(bool polling)
        => VerifyQueuedMove(new EqualValue(1), new EqualValue(1), new EqualValue(3),
            static (left, right) => left.Id == right.Id, polling, false);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public Task WrappedSource_QueuedEqualReferenceMoveWaitsForReconciliation(bool polling)
        => VerifyQueuedMove(new EqualReference(1), new EqualReference(2), new EqualReference(3),
            static (left, right) => ReferenceEquals(left, right), polling, false, true);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SubscribeWhileDeliveryIsBlocked_UsesCommittedSnapshotVersion(bool polling)
    {
        using var source = new SourceList<EqualReference>();
        var first = new EqualReference(1);
        var second = new EqualReference(2);
        source.AddRange(new[] { first, second });
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTimeOffset.FromUnixTimeMilliseconds(0).Ticks);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var blocker = source.Connect().Subscribe(changes =>
        {
            if (changes.Any(change => change.Reason == ListChangeReason.Moved))
            {
                entered.TrySetResult();
                release.Wait();
            }
        });
        var writer = Task.Factory.StartNew(() => source.Edit(list => list.Move(0, 1)),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        IDisposable? expiration = null;
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            expiration = source.ExpireAfter(item => TimeSpan.FromMilliseconds(item.Id == 1 ? 10 : 100),
                    pollingInterval: polling ? TimeSpan.FromMilliseconds(10) : null, scheduler: scheduler)
                .RecordValues(out var results, scheduler);
            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);
            await Assert.That(source.Count).IsEqualTo(1);
            await Assert.That(ReferenceEquals(source.Items[0], second)).IsTrue();
            await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
            await Assert.That(ReferenceEquals(results.RecordedValues[0].Single(), first)).IsTrue();
            release.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(30));
            scheduler.AdvanceTo(DateTimeOffset.FromUnixTimeMilliseconds(100).Ticks + 1);
            await Assert.That(source.Count).IsEqualTo(0);
            await Assert.That(results.RecordedValues.Count).IsEqualTo(2);
            await Assert.That(results.Error).IsNull();
        }
        finally
        {
            release.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(30));
            expiration?.Dispose();
        }
    }

    private static async Task VerifyQueuedMove<T>(T first, T second, T sentinel,
        Func<T, T, bool> sameOccurrence, bool polling, bool blockSentinel, bool wrapSource = false)
        where T : notnull
    {
        using var source = new SourceList<T>();
        source.AddRange(new[] { first, second });
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTimeOffset.FromUnixTimeMilliseconds(0).Ticks);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var blocker = source.Connect().Subscribe(changes =>
        {
            if (changes.Any(change => change.Reason ==
                (blockSentinel ? ListChangeReason.Add : ListChangeReason.Moved)))
            {
                entered.TrySetResult();
                release.Wait();
            }
        });
        var selectorCalls = 0;
        ISourceList<T> expirationSource = wrapSource ? new WrappedSource<T>(source) : source;
        using var expiration = expirationSource.ExpireAfter(_ => ++selectorCalls switch
            {
                1 => TimeSpan.FromMilliseconds(10),
                2 => TimeSpan.FromMilliseconds(100),
                _ => null
            }, pollingInterval: polling ? TimeSpan.FromMilliseconds(10) : null, scheduler: scheduler)
            .RecordValues(out var results, scheduler);

        var writer = Task.Factory.StartNew(() =>
        {
            if (blockSentinel)
                source.Add(sentinel);
            else
                source.Edit(list => list.Move(0, 1));
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            if (blockSentinel)
                source.Edit(list => list.Move(0, 1));

            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(20).Ticks);
            // Assert references/actual value fields, because all test rows compare equal.
            await Assert.That(sameOccurrence(source.Items[0], second)).IsTrue();
            await Assert.That(source.Count).IsEqualTo(blockSentinel ? 3 : 2);
            await Assert.That(sameOccurrence(source.Items[1], first)).IsTrue();
            await Assert.That(results.RecordedValues).IsEmpty();
        }
        finally
        {
            release.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(30));
        }

        scheduler.AdvanceBy(1);
        await Assert.That(source.Count).IsEqualTo(blockSentinel ? 2 : 1);
        await Assert.That(sameOccurrence(source.Items[0], second)).IsTrue();
        await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
        await Assert.That(sameOccurrence(results.RecordedValues[0].Single(), first)).IsTrue();
        // The short deadline must survive delivery delay; the long deadline must stay attached
        // to its own occurrence even when the two rows are the same reference or equal values.
        scheduler.AdvanceTo(DateTimeOffset.FromUnixTimeMilliseconds(99).Ticks);
        await Assert.That(source.Count).IsEqualTo(blockSentinel ? 2 : 1);
        scheduler.AdvanceTo(DateTimeOffset.FromUnixTimeMilliseconds(100).Ticks + 1);
        await Assert.That(source.Count).IsEqualTo(blockSentinel ? 1 : 0);
        await Assert.That(results.RecordedValues.Count).IsEqualTo(2);
        await Assert.That(sameOccurrence(results.RecordedValues[1].Single(), second)).IsTrue();
        await Assert.That(results.Error).IsNull();
    }

    private sealed class WrappedSource<T>(SourceList<T> inner) : ISourceList<T>
        where T : notnull
    {
        public int Count => inner.Count;
        public IReadOnlyList<T> Items => inner.Items;
        public IObservable<int> CountChanged => inner.CountChanged;
        public IObservable<IChangeSet<T>> Connect(Func<T, bool>? predicate = null) => inner.Connect(predicate);
        public IObservable<IChangeSet<T>> Preview(Func<T, bool>? predicate = null) => inner.Preview(predicate);
        public void Edit(Action<IExtendedList<T>> updateAction) => inner.Edit(updateAction);
        public void Dispose() => inner.Dispose();
    }

    private sealed class EqualReference(int id)
    {
        public int Id { get; } = id;
        public override bool Equals(object? obj) => obj is EqualReference;
        public override int GetHashCode() => 0;
    }

    private readonly struct EqualValue(int id) : IEquatable<EqualValue>
    {
        public int Id { get; } = id;
        public bool Equals(EqualValue other) => true;
        public override bool Equals(object? obj) => obj is EqualValue;
        public override int GetHashCode() => 0;
    }
}

public sealed class SourceListExpirationVersionFixture
{
    [Test]
    public async Task ConnectDuringNestedEdit_UsesCompletedSnapshot()
    {
        using var source = new SourceList<int>();
        IDisposable? connection = null;
        ListItemRecordingObserver<int>? results = null;
        source.Edit(outer =>
        {
            outer.Add(1);
            source.Edit(inner =>
            {
                inner.Add(2);
                connection = source.Connect().RecordListItems(out results);
            });
            outer.Add(3);
        });
        using (connection!)
        {
            await Assert.That(results!.RecordedChangeSets.Count).IsEqualTo(1);
            await Assert.That(results.RecordedItems.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConnectWhileDeliveryIsBlocked_SnapshotSkipsAlreadyCommittedChanges(bool filtered)
    {
        using var source = new SourceList<int>();
        source.AddRange(new[] { 1, 2 });
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        using var blocker = source.Connect().Subscribe(changes =>
        {
            if (changes.Any(change => change.Reason == ListChangeReason.Add && change.Item.Current == 3))
            {
                entered.TrySetResult();
                release.Wait();
            }
        });
        var writer = Task.Factory.StartNew(() => source.Add(3),
            CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        IDisposable? connection = null;
        try
        {
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            source.Edit(list => list.Move(0, 1));
            connection = source.Connect(filtered ? static value => value % 2 == 0 : null)
                .RecordListItems(out var results);
            await Assert.That(results.RecordedItems.SequenceEqual(filtered ? new[] { 2 } : new[] { 2, 1, 3 })).IsTrue();
            release.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(30));
            await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(1);
            source.Add(4);
            await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
            await Assert.That(results.RecordedItems.SequenceEqual(filtered ? new[] { 2, 4 } : new[] { 2, 1, 3, 4 })).IsTrue();
            await Assert.That(results.Error).IsNull();
        }
        finally
        {
            release.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(30));
            connection?.Dispose();
        }
    }
}
