using System.Diagnostics;
using System.Text.Json;

namespace DynamicData.Tests.List;

/// <summary>Bounded diagnostic probe; timings are observations, never test assertions.</summary>
[NotInParallel]
public sealed class SortSingleOutlierProbeFixture
{
    [Test]
    [Arguments(1_000, false)]
    [Arguments(10_000, false)]
    [Arguments(100_000, false)]
    [Arguments(1_000, true)]
    [Arguments(10_000, true)]
    [Arguments(100_000, true)]
    public async Task MeasureFullListSingleOutlier(int count, bool changeComparer)
    {
        using var source = new SourceList<Row>();
        using var resort = new ReactiveUI.Primitives.Signals.Signal<Unit>();
        using var comparers = new ReactiveUI.Primitives.Signals.Signal<IComparer<Row>>();
        var comparer = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
        IChangeSet<Row>? latest = null;
        using var subscription = source.Connect().Sort(comparer, resort: resort, comparerChanged: comparers, resetThreshold: int.MaxValue)
            .Subscribe(changes => latest = changes);
        var rows = Enumerable.Range(0, count).Select(static value => new Row(value)).ToArray();
        source.AddRange(rows);
        rows[0].Key = count;
        var beforeAllocation = GC.GetAllocatedBytesForCurrentThread();
        var started = Stopwatch.GetTimestamp();
        if (changeComparer)
            comparers.OnNext(comparer);
        else
            resort.OnNext(Unit.Default);
        var elapsed = Stopwatch.GetElapsedTime(started);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - beforeAllocation;

        var changes = latest!;
        // This is a synthetic [rowId, oldIndex, newIndex] delta, not a viewport or binding measurement.
        var deltaBytes = JsonSerializer.SerializeToUtf8Bytes(changes.Select(static change => new[]
        {
            change.Item.Current.Id, change.Item.PreviousIndex, change.Item.CurrentIndex
        })).Length;
        Console.WriteLine($"SORT_PROBE count={count} comparerChange={changeComparer} moves={changes.Moves} allocatedBytes={allocated} elapsedMs={elapsed.TotalMilliseconds:F3} syntheticDeltaBytes={deltaBytes}");
        var replay = rows.ToList();
        foreach (var change in changes)
        {
            var row = replay[change.Item.PreviousIndex];
            replay.RemoveAt(change.Item.PreviousIndex);
            replay.Insert(change.Item.CurrentIndex, row);
        }

        await Assert.That(changes.Count).IsEqualTo(changes.Moves);
        await Assert.That(replay).IsEquivalentTo(rows.Skip(1).Append(rows[0]), TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    private sealed class Row(int id)
    {
        public int Id { get; } = id;

        public int Key { get; set; } = id;
    }
}
