// A bounded diagnostic benchmark: five samples per case, no downstream list materialization.
// Run from the repository root with the locked SDK environment:
// dotnet run --project eng/benchmarks/key-identity -c Release [-p:UseReactive=true]
using System.Diagnostics;
#if REACTIVE_BENCHMARK
using DynamicData.Reactive;
using System.Reactive.Linq;
#else
using DynamicData;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives;
#endif

const int samples = 5;
Console.WriteLine($"Flavor: {typeof(SourceCache<,>).Namespace}; runtime: {Environment.Version}; samples: {samples}");
Console.WriteLine("rows,order,projection,median_ms,median_allocated_bytes,median_retained_subscription_bytes");
Run(1_000, "clear", true); // JIT the projection before measuring.
foreach (var count in new[] { 25, 100_000 })
{
    foreach (var order in new[] { "clear", "reverse", "shuffled" })
    {
        foreach (var keyed in new[] { false, true })
        {
            var results = Enumerable.Range(0, samples).Select(_ => Run(count, order, keyed)).ToArray();
            Console.WriteLine($"{count},{order},{(keyed ? "RemoveKey" : "source")},{Median(results.Select(result => result.Milliseconds)):F3},{Median(results.Select(result => (double)result.Allocated)):F0},{Median(results.Select(result => (double)result.Retained)):F0}");
        }
    }
}

static double Median(IEnumerable<double> values) => values.Order().ElementAt(samples / 2);

static (double Milliseconds, long Allocated, long Retained) Run(int count, string order, bool keyed)
{
    var values = Enumerable.Range(0, count).ToArray();
    var removals = (int[])values.Clone();
    if (order == "reverse")
        Array.Reverse(removals);
    else if (order == "shuffled")
        new Random(1192).Shuffle(removals);
    using var source = new SourceCache<int, int>(item => item);
    source.AddOrUpdate(values);
    var beforeSubscription = GC.GetTotalMemory(true);
    var emitted = 0;
    var valid = true;
    using var subscription = keyed
        ? source.Connect().RemoveKey().Subscribe(changes =>
        {
            foreach (var change in changes)
            {
                if (change.Reason != ListChangeReason.Remove)
                    continue;
                // Clear removes the first entry each time; reverse removes the last. Shuffled ranks are
                // exercised by the regression oracle, while this case checks the total and index range.
                var expected = order == "clear" ? 0 : count - emitted - 1;
                valid &= change.Item.CurrentIndex >= 0 && change.Item.CurrentIndex < count - emitted;
                if (order != "shuffled")
                    valid &= change.Item.CurrentIndex == expected;
                ++emitted;
            }
        })
        : source.Connect().Subscribe(changes => emitted += changes.Removes);
    var retained = GC.GetTotalMemory(true) - beforeSubscription;
    var beforeAllocation = GC.GetAllocatedBytesForCurrentThread();
    var stopwatch = Stopwatch.StartNew();
    if (order == "clear")
        source.Clear();
    else
        source.RemoveKeys(removals);
    stopwatch.Stop();
    var allocated = GC.GetAllocatedBytesForCurrentThread() - beforeAllocation;
    if (emitted != count || source.Count != 0 || !valid)
        throw new InvalidOperationException("Removal count or sequential indexes failed verification.");
    return (stopwatch.Elapsed.TotalMilliseconds, allocated, retained);
}
