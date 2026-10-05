#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;
using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Cache;

public class CacheControlTerminalContractFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GroupsWaitForLiveRegrouperAndRetainItems(bool immutable)
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var regroup = new Signal<Unit>();
        var completions = 0;
        var groupKeys = new List<int>();
        var person = new Person("P", 1);
        using var subscription = (immutable
            ? source.GroupWithImmutableState(p => p.Age, regroup).Select(changes => changes.Select(change => change.Key).ToArray())
            : source.Group(p => p.Age, regroup).Select(changes => changes.Select(change => change.Key).ToArray()))
            .Subscribe(keys => groupKeys.AddRange(keys), static _ => { }, () => completions++);
        source.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        source.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        person.Age = 2;
        regroup.OnNext(Unit.Default);
        await Assert.That(groupKeys.Contains(2)).IsTrue();
        regroup.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SpecifiedGroupingCompletesWhenEitherSourceEnds(bool groupSourceCompletes)
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var groups = new Signal<IDistinctChangeSet<int>>();
        var completions = 0;
        var sourceDisposed = 0;
        var groupsDisposed = 0;
        using var subscription = source.Finally(() => sourceDisposed++).Group(p => p.Age, groups.Finally(() => groupsDisposed++))
            .Subscribe(static _ => { }, static _ => { }, () => completions++);
        var person = new Person("P", 1);
        source.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        if (groupSourceCompletes) groups.OnCompleted(); else source.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(sourceDisposed).IsEqualTo(1);
        await Assert.That(groupsDisposed).IsEqualTo(1);
    }

    [Test]
    public async Task SortWaitsForSuppliedComparerAndResortStreams()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var comparers = new Signal<IComparer<Person>>();
        using var resort = new Signal<Unit>();
        var completions = 0;
        var messages = 0;
        using var subscription = source.Sort(comparers, resort).Subscribe(_ => messages++, static _ => { }, () => completions++);
        comparers.OnNext(SortExpressionComparer<Person>.Ascending(p => p.Age));
        var person = new Person("P", 1);
        source.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        source.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        comparers.OnNext(SortExpressionComparer<Person>.Descending(p => p.Age));
        comparers.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        resort.OnNext(Unit.Default);
        resort.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(messages > 0).IsTrue();
    }

    [Test]
    public async Task SortAndBindComparerErrorTerminatesAndDisposesSource()
    {
        using var source = new Signal<IChangeSet<Person, string>>();
        using var comparers = new Signal<IComparer<Person>>();
        var sourceDisposed = 0;
        var completions = 0;
        Exception? error = null;
        using var subscription = source.Finally(() => sourceDisposed++).SortAndBind(new List<Person>(), comparers)
            .Subscribe(static _ => { }, ex => error = ex, () => completions++);
        var expected = new InvalidOperationException("comparer");
        comparers.OnError(expected);
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(sourceDisposed).IsEqualTo(1);
    }

    [Test]
    public async Task DynamicCombineWaitsForLiveChildAfterOuterCompletion()
    {
        using var parent = new SourceList<IObservable<IChangeSet<Person, string>>>();
        using var child = new Signal<IChangeSet<Person, string>>();
        var completions = 0;
        var messages = 0;
        using var subscription = parent.Or().Subscribe(_ => messages++, static _ => { }, () => completions++);
        parent.Add(child);
        parent.Dispose();
        await Assert.That(completions).IsEqualTo(0);
        var person = new Person("P", 1);
        child.OnNext(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        await Assert.That(messages).IsEqualTo(1);
        child.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    [Test]
    public async Task StaticCombineWaitsForAllInputsAndDisposalIsSilent()
    {
        using var left = new Signal<IChangeSet<Person, string>>();
        using var right = new Signal<IChangeSet<Person, string>>();
        var completions = 0;
        using var subscription = left.Or(right).Subscribe(static _ => { }, static _ => { }, () => completions++);
        left.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        right.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
        var disposedCompletions = 0;
        new Signal<IChangeSet<Person, string>>().Or(Observable.Never<IChangeSet<Person, string>>())
            .Subscribe(static _ => { }, static _ => { }, () => disposedCompletions++).Dispose();
        await Assert.That(disposedCompletions).IsEqualTo(0);
    }

    [Test]
    public async Task EmptyStaticCombineCompletesImmediately()
    {
        var completions = 0;
        using var subscription = new List<IObservable<IChangeSet<Person, string>>>().Or()
            .Subscribe(static _ => { }, static _ => { }, () => completions++);
        await Assert.That(completions).IsEqualTo(1);
    }

    [Test]
    [NotInParallel]
    public async Task StaticCombineSerializesContendingProducersAndReplaysExactContents()
    {
        using var left = new SourceCache<Person, string>(p => p.Name);
        using var right = new SourceCache<Person, string>(p => p.Name);
        using var start = new Barrier(2);
        var active = 0;
        var overlaps = 0;
        var completions = 0;
        var contents = new Dictionary<string, Person>();
        Exception? error = null;
        using var subscription = left.Connect().Or(right.Connect()).Subscribe(RawAnonymousObserver.Create<IChangeSet<Person, string>>(
            changes =>
            {
                if (Interlocked.Increment(ref active) != 1) Interlocked.Increment(ref overlaps);
                try
                {
                    Thread.SpinWait(1000);
                    foreach (var change in changes)
                        if (change.Reason == ChangeReason.Remove) contents.Remove(change.Key);
                        else if (change.Reason is ChangeReason.Add or ChangeReason.Update) contents[change.Key] = change.Current;
                }
                finally { Interlocked.Decrement(ref active); }
            }, ex => error = ex, () => completions++));
        Task Write(SourceCache<Person, string> source, string prefix) => Task.Run(() =>
        {
            start.SignalAndWait(TimeSpan.FromSeconds(10));
            for (var i = 0; i < 200; i++) source.AddOrUpdate(new Person(prefix + i, i));
            for (var i = 0; i < 100; i++) source.Remove(prefix + i);
        });
        await Task.WhenAll(Write(left, "L"), Write(right, "R")).WaitAsync(TimeSpan.FromSeconds(30));
        left.Dispose();
        right.Dispose();
        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(error).IsNull();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(contents.Count).IsEqualTo(200);
        await Assert.That(contents.Keys.OrderBy(static key => key)).IsEquivalentTo(
            Enumerable.Range(100, 100).Select(i => "L" + i).Concat(Enumerable.Range(100, 100).Select(i => "R" + i)).OrderBy(static key => key));
    }

    [Test]
    public async Task SynchronousTreeDeliversRootSnapshotBeforeCompletion()
    {
        var person = new Person("P", 1, parentName: "root");
        var events = new List<string>();
        var source = Observable.Return<IChangeSet<Person, string>>(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        using var subscription = source.TransformToTree(p => p.ParentName)
            .Subscribe(changes => events.Add(string.Join(",", changes.Select(change => change.Current.Item.Name))), static _ => { }, () => events.Add("completed"));
        await Assert.That(events.SequenceEqual(new[] { "P", "completed" })).IsTrue();
    }

    [Test]
    public async Task SynchronousSpecifiedGroupingDeliversPopulatedSnapshotBeforeCompletion()
    {
        var person = new Person("P", 1);
        var events = new List<string>();
        var source = Observable.Return<IChangeSet<Person, string>>(new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, person.Name, person)]));
        var groups = Observable.Return<IDistinctChangeSet<int>>(new DistinctChangeSet<int>([new Change<int, int>(ChangeReason.Add, 1, 1)]));
        using var subscription = source.Group(p => p.Age, groups)
            .Subscribe(changes => events.Add(string.Join(",", changes.SelectMany(change => change.Current.Cache.Items).Select(p => p.Name))), static _ => { }, () => events.Add("completed"));
        await Assert.That(events.SequenceEqual(new[] { "P", "completed" })).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DynamicCombineFailsOnChildErrorAndDisposesOwnership(bool outerCompleted)
    {
        using var parent = new SourceList<IObservable<IChangeSet<Person, string>>>();
        using var child = new Signal<IChangeSet<Person, string>>();
        var childDisposals = 0;
        var completions = 0;
        Exception? error = null;
        using var subscription = parent.Or().Subscribe(static _ => { }, ex => error = ex, () => completions++);
        parent.Add(child.Finally(() => childDisposals++));
        if (outerCompleted) parent.Dispose();
        var expected = new InvalidOperationException("dynamic child");
        child.OnError(expected);
        await Assert.That(error).IsSameReferenceAs(expected);
        await Assert.That(completions).IsEqualTo(0);
        await Assert.That(childDisposals).IsEqualTo(1);
    }

    [Test]
    [NotInParallel]
    public async Task DynamicCombineAllowsBidirectionalChildCallbacksWithoutHoldingGates()
    {
        using var leftParent = new SourceList<IObservable<IChangeSet<Person, string>>>();
        using var rightParent = new SourceList<IObservable<IChangeSet<Person, string>>>();
        using var left = new Signal<IChangeSet<Person, string>>();
        using var right = new Signal<IChangeSet<Person, string>>();
        using var rendezvous = new Barrier(2);
        var leftValues = new List<string>();
        var rightValues = new List<string>();
        var errors = new List<Exception>();
        var leftCompleted = 0;
        var rightCompleted = 0;
        static IChangeSet<Person, string> Added(string key)
        {
            var person = new Person(key, 1);
            return new ChangeSet<Person, string>([new Change<Person, string>(ChangeReason.Add, key, person)]);
        }
        using var leftSubscription = leftParent.Or().Subscribe(changes =>
        {
            leftValues.AddRange(changes.Select(change => change.Key));
            if (changes.Any(change => change.Key == "leftTrigger"))
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("left callback rendezvous");
                right.OnNext(Added("fromLeft"));
            }
        }, errors.Add, () => leftCompleted++);
        using var rightSubscription = rightParent.Or().Subscribe(changes =>
        {
            rightValues.AddRange(changes.Select(change => change.Key));
            if (changes.Any(change => change.Key == "rightTrigger"))
            {
                if (!rendezvous.SignalAndWait(TimeSpan.FromSeconds(10))) throw new TimeoutException("right callback rendezvous");
                left.OnNext(Added("fromRight"));
            }
        }, errors.Add, () => rightCompleted++);
        leftParent.Add(left);
        rightParent.Add(right);
        await Task.WhenAll(Task.Run(() => left.OnNext(Added("leftTrigger"))), Task.Run(() => right.OnNext(Added("rightTrigger"))))
            .WaitAsync(TimeSpan.FromSeconds(30));
        leftParent.Dispose();
        rightParent.Dispose();
        left.OnCompleted();
        right.OnCompleted();
        await Assert.That(errors.Count).IsEqualTo(0);
        await Assert.That(leftValues.SequenceEqual(new[] { "leftTrigger", "fromRight" })).IsTrue();
        await Assert.That(rightValues.SequenceEqual(new[] { "rightTrigger", "fromLeft" })).IsTrue();
        await Assert.That(leftCompleted).IsEqualTo(1);
        await Assert.That(rightCompleted).IsEqualTo(1);
    }
}
