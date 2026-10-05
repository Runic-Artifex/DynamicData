namespace DynamicData.Tests.List;

public class BatchConvenienceFixture
{
    [Test]
    public async Task BufferPreservesIndexedChangesAndSkipsEmptyWindows()
    {
        var scheduler = new TestScheduler();
        using var source = new SourceList<int>();
        using var results = source.Connect().Batch(TimeSpan.FromSeconds(1), scheduler).AsAggregator();
        source.AddRange(new[] { 3, 1, 1 });
        source.Move(0, 2);
        source.ReplaceAt(1, 2);
        source.RemoveAt(0);
        source.Add(4);
        await Assert.That(results.Messages.Count).IsEqualTo(0);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(results.Messages.Count).IsEqualTo(1);
        await Assert.That(results.Data.Items.SequenceEqual(source.Items)).IsTrue();
        await Assert.That(results.Data.Items.SequenceEqual(new[] { 2, 3, 4 })).IsTrue();
        await Assert.That(results.Messages[0].Count).IsEqualTo(5);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);
        await Assert.That(results.Messages.Count).IsEqualTo(1);
    }

    [Test]
    public async Task CompletionFlushesPendingChangesBeforeCompleting()
    {
        var scheduler = new TestScheduler();
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        var notifications = new List<string>();
        using var subscription = source.Batch(TimeSpan.FromSeconds(1), scheduler).Subscribe(
            changes => notifications.Add($"next:{changes.Count}"),
            _ => notifications.Add("error"),
            () => notifications.Add("completed"));
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });
        source.OnCompleted();
        scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);

        await Assert.That(notifications.SequenceEqual(new[] { "next:1", "completed" })).IsTrue();
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task ErrorDiscardsPendingChangesAndPropagatesOriginalException()
    {
        var scheduler = new TestScheduler();
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        var error = new InvalidOperationException("source failure");
        Exception? received = null;
        var messages = 0;
        var completed = false;
        using var subscription = source.Batch(TimeSpan.FromSeconds(1), scheduler).Subscribe(
            _ => messages++, ex => received = ex, () => completed = true);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });
        source.OnError(error);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);

        await Assert.That(ReferenceEquals(received, error)).IsTrue();
        await Assert.That(messages).IsEqualTo(0);
        await Assert.That(completed).IsFalse();
        await Assert.That(source.HasObservers).IsFalse();
    }

    [Test]
    public async Task DisposalReleasesSourceAndDiscardsPendingChanges()
    {
        var scheduler = new TestScheduler();
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        var messages = 0;
        var completed = false;
        var subscription = source.Batch(TimeSpan.FromSeconds(1), scheduler).Subscribe(
            _ => messages++, () => completed = true);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1, 0) });
        subscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        scheduler.AdvanceBy(TimeSpan.FromSeconds(2).Ticks);
        await Assert.That(messages).IsEqualTo(0);
        await Assert.That(completed).IsFalse();
    }

    [Test]
    public async Task InvalidArgumentsAreRejectedWhenConstructingOperator()
    {
        IObservable<IChangeSet<int>> missing = null!;
        using var source = new SourceList<int>();
        var scheduler = new TestScheduler();
        await Assert.That(() => missing.Batch(TimeSpan.FromSeconds(1), scheduler)).Throws<ArgumentNullException>();
        await Assert.That(() => source.Connect().Batch(TimeSpan.FromTicks(-1), scheduler)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(source.Connect().Batch(TimeSpan.Zero, scheduler)).IsNotNull();
    }
}
