#if REACTIVE_TESTS
using DynamicData.Reactive;
using DynamicData.Reactive.Binding;
#else
using DynamicData;
using DynamicData.Binding;
#endif

namespace Runic.DynamicData.Consumers;

public class ToSortedCollectionConsumerFixture
{
    [Test]
    public async Task CacheDefaultComparerProducesIndependentOrderedSnapshots()
    {
        using var source = new SourceCache<int, int>(value => value);
        var snapshots = new List<IReadOnlyCollection<int>>();
        using var subscription = source.Connect().ToSortedCollection().Subscribe(snapshots.Add);

        source.AddOrUpdate(new[] { 3, 1, 2 });
        var first = snapshots[^1];
        source.Edit(updater => updater.RemoveKey(2));
        source.AddOrUpdate(0);

        await Assert.That(first.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
        await Assert.That(snapshots[^1].SequenceEqual(new[] { 0, 1, 3 })).IsTrue();
        source.Clear();
        await Assert.That(snapshots[^1].Count).IsEqualTo(0);
    }

    [Test]
    public async Task ListDefaultComparerPreservesDuplicatesAndIndependentSnapshots()
    {
        using var source = new SourceList<int>();
        var snapshots = new List<IReadOnlyCollection<int>>();
        using var subscription = source.Connect().ToSortedCollection().Subscribe(snapshots.Add);

        source.AddRange(new[] { 3, 1, 2, 1 });
        var first = snapshots[^1];
        source.RemoveAt(1);
        source.Add(0);

        await Assert.That(first.SequenceEqual(new[] { 1, 1, 2, 3 })).IsTrue();
        await Assert.That(snapshots[^1].SequenceEqual(new[] { 0, 1, 2, 3 })).IsTrue();
        source.Clear();
        await Assert.That(snapshots[^1].Count).IsEqualTo(0);
    }

    [Test]
    public async Task DefaultComparerAndHistoricalCallShapesResolveOutsideLibraryNamespace()
    {
        using var cache = new SourceCache<int, int>(value => value);
        using var list = new SourceList<int>();
        var cached = cache.Connect();
        var listed = list.Connect();
        var comparer = Comparer<int>.Create((left, right) => right.CompareTo(left));
        var shapes = new[]
        {
            cached.ToSortedCollection(),
            cached.ToSortedCollection(comparer),
            cached.ToSortedCollection(comparer: comparer),
            cached.ToSortedCollection(value => value),
            cached.ToSortedCollection(sort: value => value, sortOrder: SortDirection.Descending),
            listed.ToSortedCollection(),
            listed.ToSortedCollection(comparer),
            listed.ToSortedCollection(comparer: comparer),
            listed.ToSortedCollection(value => value),
            listed.ToSortedCollection(sort: value => value, sortOrder: SortDirection.Descending),
        };
        var snapshots = new List<int[]>();
        using var subscriptions = new CompositeDisposable(shapes.Select(shape => shape.Subscribe(items => snapshots.Add(items.ToArray()))).ToArray());
        cache.AddOrUpdate(new[] { 3, 1, 2 });
        list.AddRange(new[] { 3, 1, 2 });

        await Assert.That(snapshots.Count).IsEqualTo(shapes.Length);
        await Assert.That(snapshots[0].SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
        await Assert.That(snapshots[1].SequenceEqual(new[] { 3, 2, 1 })).IsTrue();
        await Assert.That(snapshots[5].SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
        await Assert.That(snapshots[6].SequenceEqual(new[] { 3, 2, 1 })).IsTrue();
    }

    [Test]
    public async Task DefaultComparerOverloadsRejectNullSources()
    {
        IObservable<IChangeSet<int, int>> cached = null!;
        IObservable<IChangeSet<int>> listed = null!;

        await Assert.That(() => cached.ToSortedCollection()).Throws<ArgumentNullException>();
        await Assert.That(() => listed.ToSortedCollection()).Throws<ArgumentNullException>();
    }
}
