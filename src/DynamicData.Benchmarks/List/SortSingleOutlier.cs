using ReactiveUI.Primitives.Signals;

namespace DynamicData.Benchmarks.List;

/// <summary>Measures one full-list resort, including the changes emitted for a single outlier.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3, invocationCount: 1)]
public class SortSingleOutlier
{
    private readonly IComparer<Row> _comparer = Comparer<Row>.Create(static (left, right) => left.Key.CompareTo(right.Key));
    private SourceList<Row> _source = null!;
    private Signal<Unit> _resort = null!;
    private Signal<IComparer<Row>> _comparers = null!;
    private IDisposable _subscription = null!;
    private Row[] _rows = null!;
    private int _emittedMoves;

    [Params(1_000, 10_000, 100_000)]
    public int Count { get; set; }

    [Params(false, true)]
    public bool ChangeComparer { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _source = new SourceList<Row>();
        _resort = new Signal<Unit>();
        _comparers = new Signal<IComparer<Row>>();
        _rows = Enumerable.Range(0, Count).Select(static value => new Row(value)).ToArray();
        _subscription = _source.Connect().Sort(_comparer, resort: _resort, comparerChanged: _comparers, resetThreshold: int.MaxValue)
            .Subscribe(changes => _emittedMoves = changes.Moves);
        _source.AddRange(_rows);
    }

    [IterationSetup]
    public void Restore()
    {
        _rows[0].Key = 0;
        _resort.OnNext(Unit.Default);
        _emittedMoves = 0;
    }

    [Benchmark]
    public int MoveFirstRowToEnd()
    {
        _rows[0].Key = Count;
        if (ChangeComparer)
            _comparers.OnNext(_comparer);
        else
            _resort.OnNext(Unit.Default);
        return _emittedMoves;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _subscription.Dispose();
        _source.Dispose();
        _resort.Dispose();
        _comparers.Dispose();
    }

    private sealed class Row(int key)
    {
        public int Key { get; set; } = key;
    }
}
