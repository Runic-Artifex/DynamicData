using ReactiveUI.Primitives.Signals;
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
        await Assert.That(result.Messages.Any(batch => batch is IChangeSet<int, int, VirtualContext<int>>)).IsFalse();
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
