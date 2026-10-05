using DynamicData.Aggregation;

namespace DynamicData.Benchmarks;

/// <summary>Measures the state cost of refresh-aware Sum over valid immutable replacement streams.</summary>
[MemoryDiagnoser]
[MarkdownExporterAttribute.GitHub]
public class Sum_Contracts
{
    private IChangeSet<Row, int>[] _cacheChanges = null!;
    private IChangeSet<Row>[] _listChanges = null!;

    [Params(1000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rows = Enumerable.Range(1, Count).Select(id => new Row(id, id)).ToArray();
        var replacements = rows.Select(row => row with { Value = row.Value + 1 }).ToArray();
        var cache = new ChangeAwareCache<Row, int>();
        foreach (var row in rows) cache.AddOrUpdate(row, row.Id);
        var addCache = cache.CaptureChanges();
        foreach (var row in replacements) cache.AddOrUpdate(row, row.Id);
        var replaceCache = cache.CaptureChanges();
        cache.Remove(replacements.Select(row => row.Id));
        _cacheChanges = [addCache, replaceCache, cache.CaptureChanges()];
        var list = new ChangeAwareList<Row>();
        list.AddRange(rows);
        var addList = list.CaptureChanges();
        for (var index = 0; index < Count; index++) list[index] = replacements[index];
        var replaceList = list.CaptureChanges();
        list.Clear();
        _listChanges = [addList, replaceList, list.CaptureChanges()];

        // Replay both contracts before measuring; every change references its correct immutable snapshot.
        if (RunCache(false, true) != 0 || RunCache(true, true) != 0 || RunList(false, true) != 0 || RunList(true, true) != 0)
            throw new InvalidOperationException("Sum benchmark replay did not return to zero.");
    }

    [Benchmark(Baseline = true)]
    public int CacheMutable() => RunCache(false);

    [Benchmark]
    public int CacheImmutable() => RunCache(true);

    [Benchmark]
    public int ListMutable() => RunList(false);

    [Benchmark]
    public int ListImmutable() => RunList(true);

    private int RunCache(bool immutable, bool verify = false)
    {
        using var source = new Signal<IChangeSet<Row, int>>();
        var result = 0;
        using var subscription = (immutable ? source.SumImmutable(row => row.Value) : source.Sum(row => row.Value)).Subscribe(value => result = value);
        for (var index = 0; index < _cacheChanges.Length; index++)
        {
            source.OnNext(_cacheChanges[index]);
            var expected = index switch { 0 => Count * (Count + 1) / 2, 1 => Count * (Count + 1) / 2 + Count, _ => 0 };
            if (verify && result != expected) throw new InvalidOperationException("Incorrect Sum benchmark intermediate value.");
        }
        source.OnCompleted();
        return result;
    }

    private int RunList(bool immutable, bool verify = false)
    {
        using var source = new Signal<IChangeSet<Row>>();
        var result = 0;
        using var subscription = (immutable ? source.SumImmutable(row => row.Value) : source.Sum(row => row.Value)).Subscribe(value => result = value);
        for (var index = 0; index < _listChanges.Length; index++)
        {
            source.OnNext(_listChanges[index]);
            var expected = index switch { 0 => Count * (Count + 1) / 2, 1 => Count * (Count + 1) / 2 + Count, _ => 0 };
            if (verify && result != expected) throw new InvalidOperationException("Incorrect Sum benchmark intermediate value.");
        }
        source.OnCompleted();
        return result;
    }

    private sealed record Row(int Id, int Value);
}
