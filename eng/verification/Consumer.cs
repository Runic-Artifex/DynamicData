using System.ComponentModel;
using ReactiveUI.Primitives.Signals;
#if REACTIVE_CONSUMER
using DynamicData.Reactive;
using DynamicData.Reactive.Binding;
using System.Reactive.Linq;
#else
using DynamicData;
using DynamicData.Binding;
using ReactiveUI.Primitives;
#endif

// Deliberately outside either library namespace: extension resolution here is
// the same as an application importing DynamicData and its observable flavor.
namespace Runic.ExternalConsumer;

internal static class Program
{
    private static int Main()
    {
        var probes = new (string Name, Action Run)[]
        {
            ("cache ordering, zero viewport, metadata and disposal", CacheViewport),
            ("property chains, conversion and observer disposal", Properties),
            ("owned transform disposal", OwnedTransforms),
            ("external virtual Switch overload and source handoff", VirtualSwitch),
        };
        var failures = 0;
        foreach (var probe in probes)
        {
            try
            {
                probe.Run();
                Console.WriteLine($"PASS {probe.Name}");
            }
            catch (Exception error)
            {
                failures++;
                Console.Error.WriteLine($"FAIL {probe.Name}: {error}");
            }
        }

        return failures == 0 ? 0 : 1;
    }

    private static void CacheViewport()
    {
        using var cache = new SourceCache<Row, int>(row => row.Id);
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 2));
        var rows = new[] { new Row(1, 20), new Row(2, 10), new Row(3, 30) };
        cache.AddOrUpdate(rows);
        VirtualContext<Row>? context = null;
        using var subscription = cache.Connect()
            .AutoRefresh(row => row.Rank)
            .SortAndVirtualize(Comparer<Row>.Create((left, right) => left.Rank.CompareTo(right.Rank)), requests)
            .Do(changes => context = changes.Context)
            .Bind(out var visible)
            .Subscribe(_ => { }, Fail);
        Equal(visible.Select(row => row.Id), 2, 1);
        rows[2].Rank = 5;
        Equal(visible.Select(row => row.Id), 3, 2);

        requests.OnNext(new VirtualRequest(0, 0));
        Equal(visible.Select(row => row.Id));
        Require(context?.Response.Size == 0 && context.Response.TotalSize == 3, "Zero viewport metadata is stale.");
        var fourth = new Row(4, 1);
        cache.AddOrUpdate(fourth);
        Require(context?.Response.Size == 0 && context.Response.TotalSize == 4, "Empty viewport lost total-count changes.");
        Equal(visible.Select(row => row.Id));
        requests.OnNext(new VirtualRequest(1, 2));
        Equal(visible.Select(row => row.Id), 3, 2);

        subscription.Dispose();
        Require(rows.Append(fourth).All(row => row.ObserverCount == 0), "AutoRefresh retained property observers.");
        cache.AddOrUpdate(new Row(5, -1));
        requests.OnNext(new VirtualRequest(0, 3));
        Equal(visible.Select(row => row.Id), 3, 2);
    }

    private static void Properties()
    {
        var first = new Row(1, 10);
        var second = new Row(2, 30);
        var parent = new Row(3, 0) { Child = first };
        var values = new List<long>();
        using var subscription = parent.WhenValueChanged(row => (long)row.Child!.Rank)
            .Subscribe(value => values.Add(value), Fail);
        first.Rank = 20;
        parent.Child = second;
        first.Rank = 99;
        second.Rank = 40;
        Equal(values, 10L, 20L, 30L, 40L);
        Require(first.ObserverCount == 0, "Replaced property-chain target retained an observer.");
        subscription.Dispose();
        Require(parent.ObserverCount == 0 && second.ObserverCount == 0, "Property-chain disposal retained observers.");
        second.Rank = 50;
        Equal(values, 10L, 20L, 30L, 40L);
    }

    private static void OwnedTransforms()
    {
        using var cache = new SourceCache<Row, int>(row => row.Id);
        var owned = new List<OwnedRow>();
        using var subscription = cache.Connect()
            .Transform(row =>
            {
                var value = new OwnedRow(row.Id);
                owned.Add(value);
                return value;
            })
            .DisposeMany()
            .Subscribe(_ => { }, Fail);
        cache.AddOrUpdate(new Row(1, 10));
        cache.AddOrUpdate(new Row(1, 20));
        Require(owned.Count == 2 && owned[0].DisposeCount == 1 && owned[1].DisposeCount == 0, "Replacement ownership was incorrect.");
        cache.RemoveKey(1);
        Require(owned[1].DisposeCount == 1, "Removal did not dispose its proxy.");
        cache.AddOrUpdate(new Row(2, 30));
        subscription.Dispose();
        Require(owned.All(row => row.DisposeCount == 1), "Subscription teardown did not dispose each proxy exactly once.");
    }

    private static void VirtualSwitch()
    {
        using var first = new SourceCache<Row, int>(row => row.Id);
        using var second = new SourceCache<Row, int>(row => row.Id);
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 2));
        var comparer = Comparer<Row>.Create((left, right) => left.Rank.CompareTo(right.Rank));
        first.AddOrUpdate(new Row(1, 10));
        second.AddOrUpdate(new Row(2, 20));
        using var feeds = new StateSignal<IObservable<IChangeSet<Row, int, VirtualContext<Row>>>>(
            first.Connect().SortAndVirtualize(comparer, requests));
        IObservable<IObservable<IChangeSet<Row, int, VirtualContext<Row>>>> source = feeds;
        var switched = source.Switch();
        // Assignment to the base interface alone would accept covariance from
        // Rx Switch. Generic inference checks the actually selected return type.
        Require(ElementType(switched) == typeof(IChangeSet<Row, int>), "Virtual Switch selected the plain observable overload.");
        using var subscription = switched.Bind(out var visible).Subscribe(_ => { }, Fail);
        Equal(visible.Select(row => row.Id), 1);
        feeds.OnNext(second.Connect().SortAndVirtualize(comparer, requests));
        Equal(visible.Select(row => row.Id), 2);
        first.AddOrUpdate(new Row(3, 5));
        Equal(visible.Select(row => row.Id), 2);
        subscription.Dispose();
        second.AddOrUpdate(new Row(4, 1));
        Equal(visible.Select(row => row.Id), 2);
    }

    private static Type ElementType<T>(IObservable<T> _) => typeof(T);

    private static void Equal<T>(IEnumerable<T> actual, params T[] expected)
    {
        var values = actual.ToArray();
        Require(values.SequenceEqual(expected), $"Expected [{string.Join(", ", expected)}], got [{string.Join(", ", values)}].");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
            throw new InvalidOperationException(message);
    }

    private static void Fail(Exception error) => throw new InvalidOperationException("Observable failed.", error);

    public sealed class Row(int id, int rank) : INotifyPropertyChanged
    {
        private PropertyChangedEventHandler? _propertyChanged;
        private int _rank = rank;
        private Row? _child;

        public int Id { get; } = id;

        public int ObserverCount { get; private set; }

        public event PropertyChangedEventHandler? PropertyChanged
        {
            add { _propertyChanged += value; ObserverCount++; }
            remove { _propertyChanged -= value; ObserverCount--; }
        }

        public int Rank
        {
            get => _rank;
            set
            {
                _rank = value;
                _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Rank)));
            }
        }

        public Row? Child
        {
            get => _child;
            set
            {
                _child = value;
                _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }
    }

    private sealed class OwnedRow(int id) : IDisposable
    {
        public int Id { get; } = id;

        public int DisposeCount { get; private set; }

        public void Dispose() => DisposeCount++;
    }
}
