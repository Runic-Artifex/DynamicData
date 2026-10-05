#if REACTIVE_TESTS
using DynamicData.Reactive.Cache.Internal;
#else
using DynamicData.Cache.Internal;
#endif

namespace DynamicData.Tests.Cache;

public class SortedChangeSetAggregatorFixture
{
    [Test]
    public async Task SortedItemsTracksOrderChangesAndRetainsEarlierReadOnlySnapshot()
    {
        using var source = new SourceCache<int, int>(value => value);
        using var comparer = new ReactiveUI.Primitives.Signals.Signal<IComparer<int>>();
        using var results = new SortedChangeSetAggregator<int, int>(source.Connect().Sort(comparer));
        await Assert.That(results.SortedItems.Count).IsEqualTo(0);

        comparer.OnNext(Comparer<int>.Default);
        source.AddOrUpdate(new[] { 3, 1, 2 });
        var first = results.SortedItems;
        await Assert.That(first.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
        await Assert.That(() => ((IList<int>)first)[0] = 99).Throws<NotSupportedException>();

        comparer.OnNext(Comparer<int>.Create((left, right) => right.CompareTo(left)));
        await Assert.That(results.Messages[^1].SortedItems.SortReason).IsEqualTo(SortReason.Reorder);
        await Assert.That(results.SortedItems.SequenceEqual(new[] { 3, 2, 1 })).IsTrue();
        await Assert.That(first.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();

        source.Edit(updater => updater.RemoveKey(2));
        await Assert.That(results.SortedItems.SequenceEqual(new[] { 3, 1 })).IsTrue();
        source.Clear();
        await Assert.That(results.SortedItems.Count).IsEqualTo(0);
        await Assert.That(first.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task SnapshotIsCopiedEvenWhenMessageContainsNoChanges()
    {
        using var messages = new ReactiveUI.Primitives.Signals.Signal<ISortedChangeSet<int, int>>();
        using var results = new SortedChangeSetAggregator<int, int>(messages);
        var items = new List<KeyValuePair<int, int>> { new(1, 1), new(2, 2) };
        var comparer = Comparer<KeyValuePair<int, int>>.Create((left, right) => left.Value.CompareTo(right.Value));
        messages.OnNext(new SortedChangeSet<int, int>(
            new KeyValueCollection<int, int>(items, comparer, SortReason.ComparerChanged, SortOptimisations.None),
            Array.Empty<Change<int, int>>()));
        var first = results.SortedItems;
        await Assert.That(first.SequenceEqual(new[] { 1, 2 })).IsTrue();

        items.Reverse();
        await Assert.That(first.SequenceEqual(new[] { 1, 2 })).IsTrue();
        messages.OnNext(new SortedChangeSet<int, int>(
            new KeyValueCollection<int, int>(items, comparer, SortReason.ComparerChanged, SortOptimisations.None),
            Array.Empty<Change<int, int>>()));
        await Assert.That(results.SortedItems.SequenceEqual(new[] { 2, 1 })).IsTrue();
        await Assert.That(first.SequenceEqual(new[] { 1, 2 })).IsTrue();
    }

    [Test]
    public async Task EmptyMessageReplacesLatestSnapshotAndDisposalStopsUpdates()
    {
        using var source = new SourceCache<int, int>(value => value);
        using var messages = new ReactiveUI.Primitives.Signals.Signal<ISortedChangeSet<int, int>>();
        using var sourceSubscription = source.Connect().Sort(Comparer<int>.Default).Subscribe(messages);
        using var results = new SortedChangeSetAggregator<int, int>(messages);
        source.AddOrUpdate(new[] { 2, 1 });
        await Assert.That(results.SortedItems.SequenceEqual(new[] { 1, 2 })).IsTrue();

        messages.OnNext(SortedChangeSet<int, int>.Empty);
        await Assert.That(results.Messages[^1].Count).IsEqualTo(0);
        await Assert.That(results.SortedItems.Count).IsEqualTo(0);
        var count = results.Messages.Count;
        results.Dispose();
        source.AddOrUpdate(3);
        await Assert.That(results.Messages.Count).IsEqualTo(count);
        await Assert.That(results.SortedItems.Count).IsEqualTo(0);
    }
}
