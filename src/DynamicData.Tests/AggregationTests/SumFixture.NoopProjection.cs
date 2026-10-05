#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public sealed class SumNoopProjectionFixture
{
    [Test]
    [Arguments(false, false, false, "Refresh")]
    [Arguments(false, false, false, "Replace")]
    [Arguments(false, false, true, "Refresh")]
    [Arguments(false, false, true, "Replace")]
    [Arguments(false, true, false, "Refresh")]
    [Arguments(false, true, false, "Replace")]
    [Arguments(false, true, true, "Refresh")]
    [Arguments(false, true, true, "Replace")]
    [Arguments(true, false, false, "Refresh")]
    [Arguments(true, false, false, "Replace")]
    [Arguments(true, false, true, "Refresh")]
    [Arguments(true, false, true, "Replace")]
    [Arguments(true, true, false, "Refresh")]
    [Arguments(true, true, false, "Replace")]
    [Arguments(true, true, true, "Refresh")]
    [Arguments(true, true, true, "Replace")]
    public async Task UnchangedFloatingProjection_PreservesSumAndLaterChangedRefresh(bool cache, bool single, bool infinity, string reason)
    {
        using var cacheSource = new TestSourceCache<Row, int>(row => row.Id);
        using var listSource = new TestSourceList<Row>();
        var magnitude = single ? 1e8 : 1e16;
        var first = new Row(1, infinity ? double.PositiveInfinity : magnitude);
        var last = new Row(3, 1D);
        var rows = new[] { first, new Row(2, infinity ? 0D : -magnitude), last };
        cacheSource.AddOrUpdate(rows);
        listSource.AddRange(rows);
        var sum = cache
            ? single ? cacheSource.Connect().Sum(row => (float)row.Value).Select(value => (double)value) : cacheSource.Connect().Sum(row => row.Value)
            : single ? listSource.Connect().Sum(row => (float)row.Value).Select(value => (double)value) : listSource.Connect().Sum(row => row.Value);
        using var subscription = sum.RecordValues(out var results);
        var expected = infinity ? double.PositiveInfinity : 1D;
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(expected);

        if (reason == "Refresh")
        {
            if (cache) cacheSource.Edit(updater => updater.Refresh(1));
            else listSource.Refresh(0);
        }
        else
        {
            var replacement = new Row(1, first.Value);
            if (cache) cacheSource.AddOrUpdate(replacement);
            else listSource.Edit(items => items[0] = replacement);
        }

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues).HasCount(2);
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(expected)
            .Because("reapplying an unchanged projection must not introduce cancellation or Infinity minus Infinity");

        last.Value = 2D;
        if (cache) cacheSource.Edit(updater => updater.Refresh(3));
        else listSource.Refresh(2);
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(infinity ? double.PositiveInfinity : 2D);
        await Assert.That(results.RecordedValues).HasCount(3);
        await Assert.That(results.Error).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task List_UnchangedReplacementStillUpdatesReferenceAndPosition(bool single)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<Row>>();
        var magnitude = single ? 1e8 : 1e16;
        var first = new Row(1, magnitude);
        var replacement = new Row(1, magnitude);
        var last = new Row(3, 1D);
        var sum = single ? source.Sum(row => (float)row.Value).Select(value => (double)value) : source.Sum(row => row.Value);
        using var subscription = sum.RecordValues(out var results);
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.AddRange, new[] { first, new Row(2, -magnitude), last }, 0) });
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Replace, replacement, first, 1, 0) });
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Refresh, replacement, 1) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(1D);

        last.Value = 2D;
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Refresh, last, 2) });
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(2D);
        // An indexless removal must find the replacement reference, now stored at its new position.
        source.OnNext(new ChangeSet<Row> { new(ListChangeReason.Remove, replacement) });
        var expected = single ? (double)(2F - (float)magnitude) : 2D - magnitude;
        await Assert.That(results.RecordedValues[^1]).IsEqualTo(expected);
        await Assert.That(results.Error).IsNull();
    }

    private sealed class Row(int id, double value)
    {
        public int Id { get; } = id;
        public double Value { get; set; } = value;
    }
}
