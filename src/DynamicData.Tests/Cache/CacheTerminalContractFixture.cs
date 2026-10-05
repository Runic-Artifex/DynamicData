#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
using DynamicData.Reactive.Kernel;
#else
using DynamicData.Binding;
using DynamicData.Kernel;
#endif
using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

/// <summary>Rx terminal contracts, including notifications during subscription and after a populated source.</summary>
public class CacheTerminalContractFixture
{
    private static readonly IComparer<Person> Comparer = SortExpressionComparer<Person>.Ascending(p => p.Age);

    public static IEnumerable<(string, bool, bool, bool)> TerminalCases()
    {
        string[] operators = ["group", "immutable", "specified", "tree", "sort", "bind", "merge", "items", "forced", "or", "and", "xor", "except", "innerMany", "leftMany", "rightMany", "fullMany"];
        foreach (var operation in operators)
        foreach (var synchronous in new[] { false, true })
        foreach (var populated in new[] { false, true })
        foreach (var fail in new[] { false, true })
            yield return (operation, synchronous, populated, fail);
    }

    [Test]
    [MethodDataSource(nameof(TerminalCases))]
    public async Task OperatorsDeliverOneTerminal(string operation, bool synchronous, bool populated, bool fail)
    {
        using var signal = new Signal<IChangeSet<Person, string>>();
        var expected = new InvalidOperationException("terminal");
        var terminal = fail ? Observable.Throw<IChangeSet<Person, string>>(expected) : Observable.Empty<IChangeSet<Person, string>>();
        var initial = Add(new Person("P", 20));
        IObservable<IChangeSet<Person, string>> source = synchronous
            ? (populated ? Observable.Return<IChangeSet<Person, string>>(initial).Concat(terminal) : terminal)
            : signal;
        var completions = 0;
        var errors = new List<Exception>();
        using var subscription = Project(operation, source).Subscribe(static _ => { }, errors.Add, () => completions++);
        if (!synchronous)
        {
            if (populated) signal.OnNext(initial);
            if (fail) signal.OnError(expected); else signal.OnCompleted();
        }

        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
        await Assert.That(errors.Count).IsEqualTo(fail ? 1 : 0);
        if (fail) await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    private static IObservable<Unit> Project(string operation, IObservable<IChangeSet<Person, string>> source) => operation switch
    {
        "group" => source.Group(p => p.Age).Select(static _ => Unit.Default),
        "immutable" => source.GroupWithImmutableState(p => p.Age).Select(static _ => Unit.Default),
        "specified" => source.Group(p => p.Age, Observable.Never<IDistinctChangeSet<int>>()).Select(static _ => Unit.Default),
        "tree" => source.TransformToTree(p => p.ParentName ?? "root").Select(static _ => Unit.Default),
        "sort" => source.Sort(Comparer, Observable.Empty<Unit>()).Select(static _ => Unit.Default),
        "bind" => source.SortAndBind(new List<Person>(), Observable.Return(Comparer)).Select(static _ => Unit.Default),
        "merge" => source.MergeMany(static _ => Observable.Return(1)).Select(static _ => Unit.Default),
        "items" => source.MergeManyItems(static _ => Observable.Return(1)).Select(static _ => Unit.Default),
        "forced" => source.Transform(p => p.Name, Observable.Empty<Unit>()).Select(static _ => Unit.Default),
        "or" => source.Or(Observable.Empty<IChangeSet<Person, string>>()).Select(static _ => Unit.Default),
        "and" => source.And(Observable.Empty<IChangeSet<Person, string>>()).Select(static _ => Unit.Default),
        "xor" => source.Xor(Observable.Empty<IChangeSet<Person, string>>()).Select(static _ => Unit.Default),
        "except" => source.Except(Observable.Empty<IChangeSet<Person, string>>()).Select(static _ => Unit.Default),
        "innerMany" => source.InnerJoinMany(source, p => p.ParentName ?? "root", static (p, _) => p.Name).Select(static _ => Unit.Default),
        "leftMany" => source.LeftJoinMany(source, p => p.ParentName ?? "root", static (p, _) => p.Name).Select(static _ => Unit.Default),
        "rightMany" => source.RightJoinMany(source, p => p.ParentName ?? "root", static (key, _, _) => key).Select(static _ => Unit.Default),
        "fullMany" => source.FullJoinMany(source, p => p.ParentName ?? "root", static (key, _, _) => key).Select(static _ => Unit.Default),
        _ => throw new ArgumentOutOfRangeException(nameof(operation))
    };

    private static ChangeSet<Person, string> Add(Person person) => new([new Change<Person, string>(ChangeReason.Add, person.Name, person)]);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WatchForwardsSourceTerminalAndDisposalIsSilent(bool fail)
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var cache = new IntermediateCache<Person, string>(source);
        var completions = 0;
        Exception? error = null;
        using var watch = cache.Watch("P").Subscribe(static _ => { }, ex => error = ex, () => completions++);
        var disposedCompletions = 0;
        cache.Watch("P").Subscribe(static _ => { }, static _ => { }, () => disposedCompletions++).Dispose();
        var expected = new InvalidOperationException("watch");
        if (fail) source.OnError(expected); else source.OnCompleted();
        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
        await Assert.That(error).IsSameReferenceAs(fail ? expected : null);
        await Assert.That(disposedCompletions).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MonitorStatusPreservesSynchronousStatusesAndTerminal(bool fail)
    {
        var expected = new InvalidOperationException("status");
        var statuses = new List<ConnectionStatus>();
        var completions = 0;
        Exception? error = null;
        var terminal = fail ? Observable.Throw<int>(expected) : Observable.Empty<int>();
        using var subscription = Observable.Return(1).Concat(terminal).MonitorStatus()
            .Subscribe(statuses.Add, ex => error = ex, () => completions++);
        await Assert.That(statuses.SequenceEqual(new[] { ConnectionStatus.Pending, ConnectionStatus.Loaded, fail ? ConnectionStatus.Errored : ConnectionStatus.Completed })).IsTrue();
        await Assert.That(completions).IsEqualTo(fail ? 0 : 1);
        await Assert.That(error).IsSameReferenceAs(fail ? expected : null);
    }

    [Test]
    public async Task SourceSizeLimitCompletesAfterQueuedEviction()
    {
        using var source = new SourceCache<Person, string>(p => p.Name);
        var events = new List<string>();
        using var subscription = source.LimitSizeTo(1, Scheduler.Immediate).Subscribe(values => events.Add(string.Join(",", values.Select(pair => pair.Key))), static _ => { }, () => events.Add("completed"));
        source.AddOrUpdate(new Person("old", 1));
        source.AddOrUpdate(new Person("new", 2));
        source.Dispose();
        await Assert.That(events.SequenceEqual(new[] { "old", "completed" })).IsTrue();
    }
}
