using ReactiveUI.Primitives.Signals;
using DynamicData.Tests;
#if REACTIVE_TESTS
using DynamicData.Reactive;
#else
using DynamicData;
#endif

// Deliberately outside DynamicData: namespace lookup must not select ordinary Rx Switch.
namespace Runic.ExternalConsumer;

public sealed class SpecializedSwitchFixture
{
    [Test]
    public async Task VirtualizedSwitchReturnsBaseKeyedChangesAndClearsSupersededRows()
    {
        using var first = new SourceCache<int, int>(value => value);
        using var second = new SourceCache<int, int>(value => value);
        first.AddOrUpdate(new[] { 1, 2 });
        second.AddOrUpdate(3);
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 2));
        using var feeds = new StateSignal<IObservable<IChangeSet<int, int, VirtualContext<int>>>>(
            first.Connect().SortAndVirtualize(Comparer<int>.Default, requests));

        // Compile-time type equality: inference must return the base keyed stream in both flavors.
        var switched = feeds.Switch();
        RequireBaseStream(ref switched);
        using var result = switched.AsAggregator();
        feeds.OnNext(second.Connect().SortAndVirtualize(Comparer<int>.Default, requests));
        await Assert.That(result.Data.Items).IsEquivalentTo(new[] { 3 });
        await Assert.That(result.Messages.Any(batch => batch.Removes == 2)).IsTrue();
        var clearingBatch = result.Messages.Single(batch => batch.Removes == 2);
        await Assert.That(clearingBatch is IChangeSet<int, int, VirtualContext<int>>).IsFalse();
    }

    [Test]
    public async Task VirtualizedSwitchHandsOffInOneBaseBatchAndKeepsLaterContext()
    {
        using var first = new SourceCache<int, int>(value => value);
        using var second = new SourceCache<int, int>(value => value);
        first.AddOrUpdate(new[] { 1, 2 });
        second.AddOrUpdate(new[] { 2, 3 });
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 2));
        using var feeds = new StateSignal<IObservable<IChangeSet<int, int, VirtualContext<int>>>>(
            first.Connect().SortAndVirtualize(Comparer<int>.Default, requests));
        using var result = feeds.Switch().AsAggregator();
        await Assert.That(result.Messages.Single() is IChangeSet<int, int, VirtualContext<int>>).IsTrue();

        feeds.OnNext(second.Connect().SortAndVirtualize(Comparer<int>.Default, requests));

        // The removals and the new viewport arrive together, as a base batch: no empty viewport in between.
        await Assert.That(result.Messages.Count).IsEqualTo(2);
        var handoff = result.Messages[1];
        await Assert.That(handoff is IChangeSet<int, int, VirtualContext<int>>).IsFalse();
        await Assert.That(handoff.Select(change => (change.Reason, change.Key, change.CurrentIndex)))
            .IsEquivalentTo(new[] { (ChangeReason.Remove, 1, -1), (ChangeReason.Update, 2, -1), (ChangeReason.Add, 3, -1) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(result.Data.Items).IsEquivalentTo(new[] { 2, 3 });

        second.AddOrUpdate(0);
        await Assert.That(result.Messages.Last() is IChangeSet<int, int, VirtualContext<int>>).IsTrue();
        await Assert.That(result.Data.Items).IsEquivalentTo(new[] { 0, 2 });
    }

    [Test]
    public async Task KeyedSwitchBeforeVirtualizationKeepsCurrentMetadata()
    {
        using var first = new SourceCache<int, int>(value => value);
        using var second = new SourceCache<int, int>(value => value);
        first.AddOrUpdate(new[] { 1, 2 });
        second.AddOrUpdate(new[] { 3, 4, 5 });
        using var feeds = new StateSignal<IObservable<IChangeSet<int, int>>>(first.Connect());
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 0));
        using var result = feeds.Switch().SortAndVirtualize(Comparer<int>.Default, requests).AsAggregator();
        feeds.OnNext(second.Connect());
        await Assert.That(result.Data.Count).IsEqualTo(0);
        await Assert.That(result.Messages.Last().Context.Response.TotalSize).IsEqualTo(3);
        requests.OnNext(new VirtualRequest(0, 1));
        await Assert.That(result.Data.Items.Single()).IsEqualTo(3);
    }

    private static void RequireBaseStream(ref IObservable<IChangeSet<int, int>> stream) { }
}
