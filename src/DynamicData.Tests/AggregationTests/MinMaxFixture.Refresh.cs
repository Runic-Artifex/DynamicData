#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public sealed class MinMaxRefreshFixture
{
    [Test]
    public async Task MutableRefreshAndRemoval_RecomputeBothExtremaForCacheAndList()
    {
        using var cache = new TestSourceCache<Row, int>(r => r.Id);
        using var list = new TestSourceList<Row>();
        var a = new Row(1, 10);
        var b = new Row(2, 20);
        cache.AddOrUpdate(new[] { a, b });
        list.AddRange(new[] { a, b });
        using var cmin = cache.Connect().Minimum(r => r.Value, -1).RecordValues(out var cacheMin);
        using var cmax = cache.Connect().Maximum(r => r.Value, -1).RecordValues(out var cacheMax);
        using var lmin = list.Connect().Minimum(r => r.Value, -1).RecordValues(out var listMin);
        using var lmax = list.Connect().Maximum(r => r.Value, -1).RecordValues(out var listMax);

        a.Value = 30;
        cache.Edit(updater => updater.Refresh(1));
        list.Refresh(0);
        await Assert.That(cacheMin.RecordedValues[^1]).IsEqualTo(20);
        await Assert.That(listMin.RecordedValues[^1]).IsEqualTo(20);
        await Assert.That(cacheMax.RecordedValues[^1]).IsEqualTo(30);
        await Assert.That(listMax.RecordedValues[^1]).IsEqualTo(30);

        a.Value = 15;
        cache.Edit(updater => updater.Refresh(1));
        list.Refresh(0);
        await Assert.That(cacheMin.RecordedValues[^1]).IsEqualTo(15);
        await Assert.That(listMin.RecordedValues[^1]).IsEqualTo(15);
        await Assert.That(cacheMax.RecordedValues[^1]).IsEqualTo(20);
        await Assert.That(listMax.RecordedValues[^1]).IsEqualTo(20);

        b.Value = -100; // The removed object's selector no longer identifies the cached maximum.
        cache.Remove(2);
        list.RemoveAt(1);
        await Assert.That(cacheMax.RecordedValues[^1]).IsEqualTo(15);
        await Assert.That(listMax.RecordedValues[^1]).IsEqualTo(15);
        cache.AddOrUpdate(new Row(1, 40));
        list.Edit(items => items[0] = new Row(1, 40));
        await Assert.That(cacheMin.RecordedValues[^1]).IsEqualTo(40);
        await Assert.That(listMin.RecordedValues[^1]).IsEqualTo(40);
        cache.Clear(); list.Clear();
        await Assert.That(cacheMin.RecordedValues[^1]).IsEqualTo(-1);
        await Assert.That(cacheMax.RecordedValues[^1]).IsEqualTo(-1);
        await Assert.That(listMin.RecordedValues[^1]).IsEqualTo(-1);
        await Assert.That(listMax.RecordedValues[^1]).IsEqualTo(-1);
    }

    [Test]
    public async Task Refresh_UnchangedExtremumRemainsDistinct_AndCompletionPropagates()
    {
        using var source = new TestSourceCache<Row, int>(r => r.Id);
        var row = new Row(1, 10);
        source.AddOrUpdate(row);
        using var sub = source.Connect().Maximum(r => r.Value).RecordValues(out var result);
        source.Edit(updater => updater.Refresh(1));
        source.Complete();
        await Assert.That(result.RecordedValues).HasCount(1);
        await Assert.That(result.HasCompleted).IsTrue();
        await Assert.That(result.Error).IsNull();
    }

    [Test]
    public async Task MinimumErrorsAndMaximumDisposal_FollowSourceLifetime()
    {
        using var cache = new TestSourceCache<Row, int>(r => r.Id);
        using var list = new TestSourceList<Row>();
        using var cacheMinSub = cache.Connect().Minimum(r => r.Value).RecordValues(out var cacheMin);
        using var listMinSub = list.Connect().Minimum(r => r.Value).RecordValues(out var listMin);
        using var cacheMaxSub = cache.Connect().Maximum(r => r.Value).RecordValues(out var cacheMax);
        using var listMaxSub = list.Connect().Maximum(r => r.Value).RecordValues(out var listMax);
        var row = new Row(1, 10);
        cache.AddOrUpdate(row); list.Add(row);
        cacheMaxSub.Dispose(); listMaxSub.Dispose();
        row.Value = 20;
        cache.Edit(updater => updater.Refresh(1)); list.Refresh(0);
        await Assert.That(cacheMax.RecordedValues).HasCount(1);
        await Assert.That(listMax.RecordedValues).HasCount(1);
        var error = new InvalidOperationException("source");
        cache.SetError(error); list.SetError(error);
        await Assert.That(cacheMin.Error).IsEqualTo(error);
        await Assert.That(listMin.Error).IsEqualTo(error);
        await Assert.That(cacheMax.Error).IsNull();
        await Assert.That(listMax.Error).IsNull();
    }

    private sealed class Row(int id, int value)
    {
        public int Id { get; } = id;
        public int Value { get; set; } = value;
    }
}
