#if REACTIVE_TESTS
using DynamicData.Reactive.Aggregation;
#else
using DynamicData.Aggregation;
#endif

namespace DynamicData.Tests.AggregationTests;

public sealed class StdDevFloatingOffsetsFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReportedFloatingOffsets_ProduceSampleDeviationOne(bool single)
    {
        using var cache = new TestSourceCache<double, double>(x => x);
        using var list = new TestSourceList<double>();
        var offset = single ? 100000D : 1e8;
        using var cacheSub = (single ? cache.Connect().StdDev(x => (float)x) : cache.Connect().StdDev(x => x, 0D)).RecordValues(out var cacheResult);
        using var listSub = (single ? list.Connect().StdDev(x => (float)x) : list.Connect().StdDev(x => x, 0D)).RecordValues(out var listResult);
        cache.AddOrUpdate(new[] { offset, offset + 1, offset + 2 });
        list.AddRange(new[] { offset, offset + 1, offset + 2 });
        await Assert.That(cacheResult.RecordedValues[^1]).IsEqualTo(1D);
        await Assert.That(listResult.RecordedValues[^1]).IsEqualTo(1D);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RandomRemovalAndReplacement_MatchCenteredOracleAndTranslation(bool single)
    {
        using var cache = new TestSourceCache<Row, int>(r => r.Id);
        using var list = new TestSourceList<Row>();
        using var centered = new TestSourceList<double>();
        var random = new Random(1196);
        var offset = single ? 100000D : 1e8;
        using var csub = (single ? cache.Connect().StdDev(r => (float)r.Value) : cache.Connect().StdDev(r => r.Value, 0D)).RecordValues(out var cresult);
        using var lsub = (single ? list.Connect().StdDev(r => (float)r.Value) : list.Connect().StdDev(r => r.Value, 0D)).RecordValues(out var lresult);
        using var zeroSub = centered.Connect().StdDev(x => x, 0D).RecordValues(out var zeroResult);
        var items = new List<Row>();
        var nextId = 0;
        for (var step = 0; step < 256; step++)
        {
            var action = items.Count < 2 ? 0 : random.Next(3);
            if (action == 0)
            {
                var row = new Row(nextId++, offset + random.Next(-32, 33));
                items.Add(row); cache.AddOrUpdate(row); list.Add(row); centered.Add(row.Value - offset);
            }
            else
            {
                var index = random.Next(items.Count);
                if (action == 1)
                {
                    var row = items[index];
                    items.RemoveAt(index); cache.Remove(row.Id); list.RemoveAt(index); centered.RemoveAt(index);
                }
                else
                {
                    var row = new Row(items[index].Id, offset + random.Next(-32, 33));
                    items[index] = row; cache.AddOrUpdate(row);
                    list.Edit(values => values[index] = row);
                    centered.Edit(values => values[index] = row.Value - offset);
                }
            }
            var expected = SampleDeviation(items.Select(r => r.Value - offset).ToArray());
            await Assert.That(Math.Abs(cresult.RecordedValues[^1] - expected)).IsLessThanOrEqualTo(1e-6);
            await Assert.That(Math.Abs(lresult.RecordedValues[^1] - expected)).IsLessThanOrEqualTo(1e-6);
            await Assert.That(Math.Abs(zeroResult.RecordedValues[^1] - expected)).IsLessThanOrEqualTo(1e-10);
        }
    }

    [Test]
    [Arguments(double.PositiveInfinity)]
    [Arguments(double.NegativeInfinity)]
    [Arguments(double.NaN)]
    public async Task NonfiniteInputs_PropagateNaNUntilPopulationIsReset(double nonfinite)
    {
        using var source = new TestSourceList<double>();
        using var sub = source.Connect().StdDev(x => x, -1D).RecordValues(out var result);
        source.AddRange(new[] { nonfinite, 1D });
        await Assert.That(double.IsNaN(result.RecordedValues[^1])).IsTrue();
        source.Clear();
        await Assert.That(result.RecordedValues[^1]).IsEqualTo(-1D);
        source.AddRange(new[] { 1D, 2D, 3D });
        await Assert.That(result.RecordedValues[^1]).IsEqualTo(1D);
        await Assert.That(result.Error).IsNull();
    }

    [Test]
    public async Task DecimalCentralMomentsOutsideRange_ReportOverflow()
    {
        using var source = new TestSourceList<decimal>();
        using var sub = source.Connect().StdDev(x => x, 0M).RecordValues(out var result);
        source.AddRange(new[] { 0M, decimal.MaxValue });
        await Assert.That(result.Error is OverflowException).IsTrue();
    }

    private static double SampleDeviation(double[] values)
    {
        if (values.Length < 2) return 0D;
        var mean = values.Average();
        return Math.Sqrt(values.Sum(value => (value - mean) * (value - mean)) / (values.Length - 1));
    }

    private sealed record Row(int Id, double Value);
}
