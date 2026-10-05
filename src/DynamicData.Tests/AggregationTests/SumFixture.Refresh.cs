#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public sealed class SumRefreshFixture
{
    [Test]
    [Arguments("int")]
    [Arguments("long")]
    [Arguments("float")]
    [Arguments("double")]
    [Arguments("decimal")]
    [Arguments("int?")]
    [Arguments("long?")]
    [Arguments("float?")]
    [Arguments("double?")]
    [Arguments("decimal?")]
    public async Task AllNumericSelectors_RefreshAndRemovalUseStoredProjections(string numericType)
    {
        using var cache = new TestSourceCache<Row, int>(row => row.Id);
        using var list = new TestSourceList<Row>();
        var row = new Row(1, 10);
        cache.AddOrUpdate(row);
        list.Add(row);
        using var cacheSub = CacheSum(cache.Connect(), numericType, false).RecordValues(out var cacheResults);
        using var listSub = ListSum(list.Connect(), numericType, false).RecordValues(out var listResults);
        using var cacheImmutableSub = CacheSum(cache.Connect(), numericType, true).RecordValues(out var cacheImmutable);
        using var listImmutableSub = ListSum(list.Connect(), numericType, true).RecordValues(out var listImmutable);

        row.Value = 20;
        cache.Edit(updater => updater.Refresh(1));
        list.Refresh(0);
        await Assert.That(cacheResults.RecordedValues[^1]).IsEqualTo(20D);
        await Assert.That(listResults.RecordedValues[^1]).IsEqualTo(20D);
        await Assert.That(cacheImmutable.RecordedValues[^1]).IsEqualTo(10D);
        await Assert.That(listImmutable.RecordedValues[^1]).IsEqualTo(10D);

        row.Value = null;
        cache.Edit(updater => updater.Refresh(1));
        list.Refresh(0);
        await Assert.That(cacheResults.RecordedValues[^1]).IsEqualTo(0D);
        await Assert.That(listResults.RecordedValues[^1]).IsEqualTo(0D);

        row.Value = 999; // Mutation without refresh must not corrupt removal of the cached projection.
        cache.Remove(1);
        list.RemoveAt(0);
        await Assert.That(cacheResults.RecordedValues[^1]).IsEqualTo(0D);
        await Assert.That(listResults.RecordedValues[^1]).IsEqualTo(0D);
        await Assert.That(cacheResults.Error).IsNull();
        await Assert.That(listResults.Error).IsNull();
    }

    [Test]
    public async Task Cache_SameReferenceUnderTwoKeysAndSubscribers_HaveIndependentProjectionState()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Row, int>>();
        var row = new Row(1, 10);
        var sum = source.Sum(r => r.Value.GetValueOrDefault());
        using var first = sum.RecordValues(out var one);
        using var second = sum.RecordValues(out var two);
        source.OnNext(new ChangeSet<Row, int> { new(ChangeReason.Add, 1, row), new(ChangeReason.Add, 2, row) });
        row.Value = 30;
        source.OnNext(new ChangeSet<Row, int> { new(ChangeReason.Refresh, 2, row) });
        await Assert.That(one.RecordedValues[^1]).IsEqualTo(40);
        await Assert.That(two.RecordedValues[^1]).IsEqualTo(40);
        source.OnNext(new ChangeSet<Row, int> { new(ChangeReason.Remove, 1, row) });
        await Assert.That(one.RecordedValues[^1]).IsEqualTo(30);
        await Assert.That(two.RecordedValues[^1]).IsEqualTo(30);
    }

    [Test]
    public async Task List_IndexlessEqualityAndDuplicateReferences_RemoveOneStoredOccurrence()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Row>>();
        var first = new Row(1, 10);
        var equal = new Row(1, 20);
        using var sub = source.Sum(r => r.Value.GetValueOrDefault()).RecordValues(out var results);
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.AddRange, new[] { first, equal, equal }) });
        equal.Value = 30;
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Refresh, equal, 1) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(60);
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.RemoveRange, new[] { equal, equal }) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(10);
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Replace, new Row(2, 40), new Row(1, 123)) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(40);
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Clear, new[] { new Row(2, 123) }) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
    }

    [Test]
    public async Task List_MoveRefreshReplaceAndIndexedRanges_TrackPositions()
    {
        using var source = new TestSourceList<Row>();
        var a = new Row(1, 10);
        var b = new Row(1, 20);
        source.AddRange(new[] { a, b, a });
        using var sub = source.Connect().Sum(r => r.Value.GetValueOrDefault()).RecordValues(out var results);
        source.Move(1, 0);
        b.Value = 50;
        source.Refresh(0);
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(70);
        source.Edit(items => items[1] = new Row(3, 30));
        source.Edit(items => items.InsertRange(new[] { new Row(4, 4), new Row(5, 5) }, 1));
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(99);
        source.Edit(items => items.RemoveRange(0, 3));
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(40);
        source.Clear();
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(0);
        await Assert.That(results.Error).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task NumericOverflowPolicy_PreservesUncheckedIntegralAndCheckedDecimal(bool immutable)
    {
        using var ints = new TestSourceList<int>();
        using var longs = new TestSourceCache<long, long>(x => x);
        using var decimals = new TestSourceList<decimal>();
        using var intSub = (immutable ? ints.Connect().SumImmutable(x => x) : ints.Connect().Sum(x => x)).RecordValues(out var intResults);
        using var longSub = (immutable ? longs.Connect().SumImmutable(x => x) : longs.Connect().Sum(x => x)).RecordValues(out var longResults);
        using var decimalSub = (immutable ? decimals.Connect().SumImmutable(x => x) : decimals.Connect().Sum(x => x)).RecordValues(out var decimalResults);
        ints.AddRange(new[] { int.MaxValue, 1 });
        longs.AddOrUpdate(new[] { long.MaxValue, 1L });
        decimals.AddRange(new[] { decimal.MaxValue, 1M });
        await Assert.That(intResults.RecordedValues[^1]).IsEqualTo(int.MinValue);
        await Assert.That(longResults.RecordedValues[^1]).IsEqualTo(long.MinValue);
        await Assert.That(decimalResults.Error is OverflowException).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Immutable_TerminalEventsAndDisposalFollowSource(bool fail)
    {
        using var cache = new TestSourceCache<Row, int>(r => r.Id);
        using var list = new TestSourceList<Row>();
        using var cacheSub = cache.Connect().SumImmutable(r => r.Value).RecordValues(out var cacheResults);
        using var listSub = list.Connect().SumImmutable(r => r.Value).RecordValues(out var listResults);
        cache.AddOrUpdate(new Row(1, 10));
        list.Add(new Row(1, 10));
        var error = new InvalidOperationException("source");
        if (fail) { cache.SetError(error); list.SetError(error); }
        else { cache.Complete(); list.Complete(); }
        await Assert.That(cacheResults.Error).IsEqualTo(fail ? error : null);
        await Assert.That(listResults.Error).IsEqualTo(fail ? error : null);
        await Assert.That(cacheResults.HasCompleted).IsEqualTo(!fail);
        await Assert.That(listResults.HasCompleted).IsEqualTo(!fail);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisposalAndResubscription_ReleaseTheOldStateAndSeedCurrentValues(bool immutable)
    {
        using var cache = new TestSourceCache<Row, int>(r => r.Id);
        using var list = new TestSourceList<Row>();
        var row = new Row(1, 10);
        cache.AddOrUpdate(row); list.Add(row);
        var cacheStream = immutable ? cache.Connect().SumImmutable(r => r.Value) : cache.Connect().Sum(r => r.Value);
        var listStream = immutable ? list.Connect().SumImmutable(r => r.Value) : list.Connect().Sum(r => r.Value);
        using var cacheSub = cacheStream.RecordValues(out var oldCache);
        using var listSub = listStream.RecordValues(out var oldList);
        cacheSub.Dispose(); listSub.Dispose();
        row.Value = 20;
        cache.Edit(updater => updater.Refresh(1)); list.Refresh(0);
        using var newCacheSub = cacheStream.RecordValues(out var newCache);
        using var newListSub = listStream.RecordValues(out var newList);
        await Assert.That(oldCache.RecordedValues).HasCount(1);
        await Assert.That(oldList.RecordedValues).HasCount(1);
        await Assert.That(newCache.RecordedValues[^1]).IsEqualTo(20);
        await Assert.That(newList.RecordedValues[^1]).IsEqualTo(20);
        row.Value = 30;
        cache.Edit(updater => updater.Refresh(1)); list.Refresh(0);
        await Assert.That(newCache.RecordedValues[^1]).IsEqualTo(immutable ? 20 : 30);
        await Assert.That(newList.RecordedValues[^1]).IsEqualTo(immutable ? 20 : 30);
    }

    private sealed class Row(int id, int? value) : IEquatable<Row>
    {
        public int Id { get; } = id;
        public int? Value { get; set; } = value;
        public bool Equals(Row? other) => other?.Id == Id;
        public override bool Equals(object? other) => other is Row row && Equals(row);
        public override int GetHashCode() => Id;
    }

    private static IObservable<double> CacheSum(IObservable<IChangeSet<Row, int>> source, string type, bool immutable)
        => (type, immutable) switch
        {
            ("int", false) => source.Sum(r => (int)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("int", true) => source.SumImmutable(r => (int)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("int?", false) => source.Sum(r => (int?)r.Value).Select(x => (double)x),
            ("int?", true) => source.SumImmutable(r => (int?)r.Value).Select(x => (double)x),
            ("long", false) => source.Sum(r => (long)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("long", true) => source.SumImmutable(r => (long)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("long?", false) => source.Sum(r => (long?)r.Value).Select(x => (double)x),
            ("long?", true) => source.SumImmutable(r => (long?)r.Value).Select(x => (double)x),
            ("float", false) => source.Sum(r => (float)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("float", true) => source.SumImmutable(r => (float)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("float?", false) => source.Sum(r => (float?)r.Value).Select(x => (double)x),
            ("float?", true) => source.SumImmutable(r => (float?)r.Value).Select(x => (double)x),
            ("double", false) => source.Sum(r => (double)r.Value.GetValueOrDefault()),
            ("double", true) => source.SumImmutable(r => (double)r.Value.GetValueOrDefault()),
            ("double?", false) => source.Sum(r => (double?)r.Value),
            ("double?", true) => source.SumImmutable(r => (double?)r.Value),
            ("decimal", false) => source.Sum(r => (decimal)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("decimal", true) => source.SumImmutable(r => (decimal)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("decimal?", false) => source.Sum(r => (decimal?)r.Value).Select(x => (double)x),
            ("decimal?", true) => source.SumImmutable(r => (decimal?)r.Value).Select(x => (double)x),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };

    private static IObservable<double> ListSum(IObservable<IChangeSet<Row>> source, string type, bool immutable)
        => (type, immutable) switch
        {
            ("int", false) => source.Sum(r => (int)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("int", true) => source.SumImmutable(r => (int)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("int?", false) => source.Sum(r => (int?)r.Value).Select(x => (double)x),
            ("int?", true) => source.SumImmutable(r => (int?)r.Value).Select(x => (double)x),
            ("long", false) => source.Sum(r => (long)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("long", true) => source.SumImmutable(r => (long)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("long?", false) => source.Sum(r => (long?)r.Value).Select(x => (double)x),
            ("long?", true) => source.SumImmutable(r => (long?)r.Value).Select(x => (double)x),
            ("float", false) => source.Sum(r => (float)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("float", true) => source.SumImmutable(r => (float)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("float?", false) => source.Sum(r => (float?)r.Value).Select(x => (double)x),
            ("float?", true) => source.SumImmutable(r => (float?)r.Value).Select(x => (double)x),
            ("double", false) => source.Sum(r => (double)r.Value.GetValueOrDefault()),
            ("double", true) => source.SumImmutable(r => (double)r.Value.GetValueOrDefault()),
            ("double?", false) => source.Sum(r => (double?)r.Value),
            ("double?", true) => source.SumImmutable(r => (double?)r.Value),
            ("decimal", false) => source.Sum(r => (decimal)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("decimal", true) => source.SumImmutable(r => (decimal)r.Value.GetValueOrDefault()).Select(x => (double)x),
            ("decimal?", false) => source.Sum(r => (decimal?)r.Value).Select(x => (double)x),
            ("decimal?", true) => source.SumImmutable(r => (decimal?)r.Value).Select(x => (double)x),
            _ => throw new ArgumentOutOfRangeException(nameof(type))
        };
}
