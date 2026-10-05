#if REACTIVE_TESTS
using DynamicData.Reactive;
#else
using DynamicData;
#endif

namespace Runic.DynamicData.Consumers;

public class BatchConsumerFixture
{
    [Test]
    public async Task CacheAndListBatchCallShapesResolveOutsideLibraryNamespace()
    {
        using var cache = new SourceCache<int, int>(value => value);
        using var list = new SourceList<int>();
        var cached = cache.Connect();
        var listed = list.Connect();
        var scheduler = new TestScheduler();
        var window = TimeSpan.FromSeconds(1);
        var cacheShapes = new[]
        {
            cached.Batch(window),
            cached.Batch(window, scheduler),
            cached.Batch(timeSpan: window, scheduler: scheduler),
        };
        var listShapes = new[]
        {
            listed.Batch(window),
            listed.Batch(window, scheduler),
            listed.Batch(timeSpan: window, scheduler: scheduler),
        };
        var cachedResults = new List<IChangeSet<int, int>>();
        var listedResults = new List<IChangeSet<int>>();
        using var cacheSubscription = cacheShapes[2].Subscribe(cachedResults.Add);
        using var listSubscription = listShapes[2].Subscribe(listedResults.Add);
        cache.AddOrUpdate(1);
        list.Add(1);
        scheduler.AdvanceBy(window.Ticks);

        await Assert.That(cachedResults.Count).IsEqualTo(1);
        await Assert.That(listedResults.Count).IsEqualTo(1);
    }
}
