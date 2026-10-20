// Copyright (c) 2011-2026 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Linq;

#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

/// <summary>Verifies that standard deviation honours Refresh and removes the value that was added (#896, #1196).</summary>
public sealed class StdDevRefreshFixture
{
    private const long LargeInt32Offset = 1_000_000_000;
    private const long LargeOffset = 1_000_000_000_000_000;

    [Test]
    [Arguments(nameof(Int32), 0L)]
    [Arguments(nameof(Int32), LargeInt32Offset)]
    [Arguments(nameof(Int64), LargeOffset)]
    [Arguments(nameof(Decimal), LargeOffset)]
    [Arguments(nameof(Double), 0L)]
    [Arguments(nameof(Double), 100_000_000L)]
    [Arguments(nameof(Single), 100_000L)]
    public async Task Cache_RefreshedAndMutatedRows_MatchCenteredReferenceOnEveryEmission(string numericType, long offset)
    {
        using var source = new TestSourceCache<Row, int>(static row => row.Id);
        using var subscription = CacheStdDev(source.Connect(), numericType).RecordValues(out var results);
        var known = new Dictionary<int, long>();
        var expected = new List<double>();
        void Expect() => expected.Add(Reference(known.Values, numericType));

        var a = new Row(1, offset + 1);
        var b = new Row(2, offset + 1);
        var c = new Row(3, offset + 4);
        var d = new Row(4, offset + 4);
        source.AddOrUpdate(new[] { a, b, c, d });
        foreach (var row in new[] { a, b, c, d })
        {
            known[row.Id] = row.Value;
        }

        Expect();

        a.Value = offset + 10;
        source.Refresh(a);
        known[a.Id] = a.Value;
        Expect();

        // An unchanged Refresh still emits once, as before.
        source.Refresh(a);
        Expect();

        b.Value = offset + 7;
        source.Refresh(b);
        known[b.Id] = b.Value;
        Expect();

        // Mutated without Refresh, then removed: the previously added value is subtracted.
        a.Value = offset + 20;
        source.Remove(a);
        known.Remove(a.Id);
        Expect();

        c.Value = offset - 3;
        source.Refresh(c);
        known[c.Id] = c.Value;
        Expect();

        // Mutated without Refresh, then replaced: the update removes the previously added value.
        c.Value = offset + 50;
        var replacement = new Row(c.Id, offset + 2);
        source.AddOrUpdate(replacement);
        known[c.Id] = replacement.Value;
        Expect();

        // Refresh mixed with membership changes in one batch.
        b.Value = offset + 4;
        var e = new Row(5, offset - 8);
        source.Edit(updater =>
        {
            updater.Refresh(b);
            updater.Remove(replacement.Id);
            updater.AddOrUpdate(e);
        });
        known[b.Id] = b.Value;
        known.Remove(replacement.Id);
        known[e.Id] = e.Value;
        Expect();

        // b and d now hold equal values.
        e.Value = offset + 100;
        source.Remove(e);
        known.Remove(e.Id);
        Expect();

        source.Remove(b);
        known.Remove(b.Id);
        Expect();

        await AssertMatches(results, expected, numericType);
    }

    [Test]
    [Arguments(nameof(Int32), 0L)]
    [Arguments(nameof(Int32), LargeInt32Offset)]
    [Arguments(nameof(Int64), LargeOffset)]
    [Arguments(nameof(Decimal), LargeOffset)]
    [Arguments(nameof(Double), 0L)]
    [Arguments(nameof(Double), 100_000_000L)]
    [Arguments(nameof(Single), 100_000L)]
    public async Task List_RefreshedAndMutatedOccurrences_MatchCenteredReferenceOnEveryEmission(string numericType, long offset)
    {
        using var source = new TestSourceList<Row>();
        using var subscription = ListStdDev(source.Connect(), numericType).RecordValues(out var results);
        var known = new List<long>();
        var expected = new List<double>();
        void Expect() => expected.Add(Reference(known, numericType));

        var a = new Row(1, offset + 1);
        var b = new Row(2, offset + 1);
        var d = new Row(4, offset + 4);

        // Equal values and a duplicated occurrence of the same reference.
        source.AddRange(new[] { a, b, d, d });
        known.AddRange(new[] { a.Value, b.Value, d.Value, d.Value });
        Expect();

        a.Value = offset + 10;
        source.Refresh(0);
        known[0] = a.Value;
        Expect();

        d.Value = offset + 6;
        source.Refresh(new[] { 2, 3 });
        known[2] = d.Value;
        known[3] = d.Value;
        Expect();

        // Mutated without Refresh, then one occurrence removed.
        d.Value = offset + 30;
        source.RemoveAt(3);
        known.RemoveAt(3);
        Expect();

        source.Refresh(2);
        known[2] = d.Value;
        Expect();

        // Mutated without Refresh, then replaced.
        a.Value = offset + 20;
        var replacement = new Row(5, offset + 2);
        source.Edit(list => list[0] = replacement);
        known[0] = replacement.Value;
        Expect();

        b.Value = offset - 5;
        source.Refresh(1);
        known[1] = b.Value;
        Expect();

        // An unchanged Refresh still emits once, as before.
        source.Refresh(1);
        Expect();

        // Refresh mixed with membership changes in one batch.
        d.Value = offset + 2;
        var e = new Row(6, offset - 8);
        source.Edit(list =>
        {
            ((ChangeAwareList<Row>)list).RefreshAt(2);
            list.RemoveAt(1);
            list.Add(e);
        });
        known[2] = d.Value;
        known.RemoveAt(1);
        known.Add(e.Value);
        Expect();

        b.Value = offset + 400;
        e.Value = offset + 400;
        source.RemoveAt(2);
        known.RemoveAt(2);
        Expect();

        source.RemoveAt(0);
        known.RemoveAt(0);
        Expect();

        await AssertMatches(results, expected, numericType);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Aggregate_RemovalSubtractsRetainedProjectionAndEmptyBatchReevaluates(bool keyed)
    {
        using var list = new TestSourceList<Row>();
        using var cache = new TestSourceCache<Row, int>(static row => row.Id);
        var aggregate = keyed ? cache.Connect().ForAggregation() : list.Connect().ForAggregation();
        using var decimals = aggregate.StdDev(static row => (decimal)row.Value, -1M).RecordValues(out var decimalResults);
        using var doubles = aggregate.StdDev(static row => (double)row.Value, -1D).RecordValues(out var doubleResults);
        var a = new Row(1, LargeOffset + 1);
        var b = new Row(2, LargeOffset + 2);
        var c = new Row(3, LargeOffset + 3);
        var rows = new[] { a, b, c };
        void Add() { if (keyed) cache.AddOrUpdate(rows); else list.AddRange(rows); }
        void RefreshFirst() { if (keyed) cache.Refresh(a); else list.Refresh(0); }
        void RemoveFirst() { if (keyed) cache.Remove(a); else list.RemoveAt(0); }

        Add();
        a.Value = LargeOffset + 7;
        RefreshFirst();
        a.Value = LargeOffset + 1000;
        RemoveFirst();

        // [1, 2, 3] -> [7, 2, 3] -> [2, 3]
        var expected = new[] { 1M, Reference(new[] { 7L, 2L, 3L }), (decimal)Math.Sqrt(0.5D) };
        await Assert.That(decimalResults.RecordedValues.Count).IsEqualTo(3);
        await Assert.That(decimalResults.RecordedValues[0]).IsEqualTo(expected[0]);
        await Assert.That(Math.Abs(decimalResults.RecordedValues[1] - expected[1])).IsLessThanOrEqualTo(1e-20M);
        await Assert.That(Math.Abs(decimalResults.RecordedValues[2] - expected[2])).IsLessThanOrEqualTo(1e-12M);
        await Assert.That(doubleResults.RecordedValues.Count).IsEqualTo(3);
        await Assert.That(decimalResults.Error).IsNull();
        await Assert.That(doubleResults.Error).IsNull();
    }

    private static IObservable<double> CacheStdDev(IObservable<IChangeSet<Row, int>> source, string numericType) => numericType switch
    {
        nameof(Int32) => source.StdDev(static row => (int)row.Value, -1),
        nameof(Int64) => source.StdDev(static row => row.Value, -1L),
        nameof(Decimal) => source.StdDev(static row => (decimal)row.Value, -1M).Select(static value => (double)value),
        nameof(Double) => source.StdDev(static row => (double)row.Value, -1D),
        nameof(Single) => source.StdDev(static row => (float)row.Value, -1F),
        _ => throw new ArgumentOutOfRangeException(nameof(numericType))
    };

    private static IObservable<double> ListStdDev(IObservable<IChangeSet<Row>> source, string numericType) => numericType switch
    {
        nameof(Int32) => source.StdDev(static row => (int)row.Value, -1),
        nameof(Int64) => source.StdDev(static row => row.Value, -1L),
        nameof(Decimal) => source.StdDev(static row => (decimal)row.Value, -1M).Select(static value => (double)value),
        nameof(Double) => source.StdDev(static row => (double)row.Value, -1D),
        nameof(Single) => source.StdDev(static row => (float)row.Value, -1F),
        _ => throw new ArgumentOutOfRangeException(nameof(numericType))
    };

    private static double Reference(IEnumerable<long> values, string numericType)
    {
        var projected = values.Select(value => numericType == nameof(Single) ? (long)(float)value : value).ToArray();
        return projected.Length < 2 ? -1D : (double)Reference(projected);
    }

    // Centered two-pass sample standard deviation in decimal.
    private static decimal Reference(IReadOnlyCollection<long> values)
    {
        var mean = values.Sum(static value => (decimal)value) / values.Count;
        var sumOfSquares = values.Sum(value => ((decimal)value - mean) * ((decimal)value - mean));
        var variance = sumOfSquares / (values.Count - 1);
        var root = (decimal)Math.Sqrt((double)variance);
        for (var iteration = 0; iteration < 8 && root != 0M; iteration++)
        {
            root = (root + (variance / root)) / 2;
        }

        return root;
    }

    private static async Task AssertMatches(ValueRecordingObserver<double> results, IReadOnlyList<double> expected, string numericType)
    {
        var tolerance = numericType is nameof(Double) or nameof(Single) ? 1e-6 : 1e-9;
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues.Count).IsEqualTo(expected.Count);
        for (var index = 0; index < expected.Count; index++)
        {
            await Assert.That(Math.Abs(results.RecordedValues[index] - expected[index]))
                .IsLessThanOrEqualTo(tolerance)
                .Because($"emission {index} expected {expected[index]} but was {results.RecordedValues[index]}");
        }
    }

    private sealed class Row(int id, long value)
    {
        public int Id { get; } = id;

        public long Value { get; set; } = value;
    }
}
