using ReactiveUI.Primitives.Signals;
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public sealed class ViewportContractFixture
{
    [Test]
    public async Task ZeroBeforePopulatedSourceNeverMaterializesDefaultRows()
    {
        using var source = new SourceCache<int, int>(value => value);
        source.AddOrUpdate(Enumerable.Range(0, 100));
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(12, 0));
        using var results = source.Connect().SortAndVirtualize(Comparer<int>.Default, requests).AsAggregator();

        await Assert.That(results.Data.Count).IsEqualTo(0);
        await Assert.That(results.Messages.All(batch => batch.Count == 0)).IsTrue();
        await Assert.That(results.Messages.Last().Context.Response.StartIndex).IsEqualTo(12);
        await Assert.That(results.Messages.Last().Context.Response.Size).IsEqualTo(0);
        await Assert.That(results.Messages.Last().Context.Response.TotalSize).IsEqualTo(100);

        source.AddOrUpdate(100);
        await Assert.That(results.Messages.Last().Context.Response.TotalSize).IsEqualTo(101);
        requests.OnNext(new VirtualRequest(12, 3));
        await Assert.That(results.Data.Items.Order()).IsEquivalentTo(new[] { 12, 13, 14 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task ZeroRemovesRowsAndInvalidRequestsDoNotPoisonLaterUpdates()
    {
        using var source = new SourceCache<int, int>(value => value);
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 3));
        using var results = source.Connect().SortAndVirtualize(Comparer<int>.Default, requests).AsAggregator();
        source.AddOrUpdate(Enumerable.Range(0, 5));
        requests.OnNext(new VirtualRequest(2, 0));
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(3);
        await Assert.That(results.Data.Count).IsEqualTo(0);
        var messageCount = results.Messages.Count;
        requests.OnNext(new VirtualRequest(-1, 2));
        requests.OnNext(new VirtualRequest(0, -1));
        requests.OnNext(null!);
        await Assert.That(results.Messages.Count).IsEqualTo(messageCount);
        source.RemoveKey(0);
        await Assert.That(results.Messages.Last().Context.Response.StartIndex).IsEqualTo(2);
        await Assert.That(results.Messages.Last().Context.Response.Size).IsEqualTo(0);
        await Assert.That(results.Messages.Last().Context.Response.TotalSize).IsEqualTo(4);
    }

    [Test]
    public async Task EmptyWindowForwardsComparerAndTerminalSignals()
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 0));
        using var comparers = new StateSignal<IComparer<int>>(Comparer<int>.Default);
        using var results = source.SortAndVirtualize(comparers, requests).AsAggregator();
        var descending = Comparer<int>.Create((left, right) => right.CompareTo(left));
        comparers.OnNext(descending);
        await Assert.That(results.Messages.Last().Context.Comparer).IsSameReferenceAs(descending);
        source.OnCompleted();
        await Assert.That(results.IsCompleted).IsFalse();
        comparers.OnCompleted();
        requests.OnCompleted();
        await Assert.That(results.IsCompleted).IsTrue();
    }

    [Test]
    public async Task ZeroRequestBeforeDelayedComparerDeliversEmptyContext()
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(7, 0));
        using var comparers = new Signal<IComparer<int>>();
        using var results = source.SortAndVirtualize(comparers, requests).AsAggregator();
        comparers.OnNext(Comparer<int>.Default);
        await Assert.That(results.Messages.Count).IsEqualTo(1);
        await Assert.That(results.Messages[0].Count).IsEqualTo(0);
        await Assert.That(results.Messages[0].Context.Response.StartIndex).IsEqualTo(7);
        await Assert.That(results.Messages[0].Context.Response.Size).IsEqualTo(0);
    }

    [Test]
    public async Task DefaultWindowRemainsBoundedToTwentyFive()
    {
        using var source = new SourceCache<int, int>(value => value);
        using var results = source.Connect().SortAndVirtualize(Comparer<int>.Default, Observable.Never<IVirtualRequest>()).AsAggregator();
        source.AddOrUpdate(Enumerable.Range(0, 100));
        await Assert.That(results.Data.Count).IsEqualTo(25);
    }

    [Test]
    public async Task LegacyCacheVirtualiseSupportsZeroAndCountOnlyChanges()
    {
        using var source = new SourceCache<Person, string>(person => person.Name);
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 2));
#if REACTIVE_TESTS
        var comparer = DynamicData.Reactive.Binding.SortExpressionComparer<Person>.Ascending(person => person.Age);
#else
        var comparer = DynamicData.Binding.SortExpressionComparer<Person>.Ascending(person => person.Age);
#endif
        using var results = source.Connect().Sort(comparer).Virtualise(requests).AsAggregator();
        source.AddOrUpdate(new[] { new Person("a", 1), new Person("b", 2) });
        requests.OnNext(new VirtualRequest(1, 0));
        await Assert.That(results.Data.Count).IsEqualTo(0);
        await Assert.That(results.Messages.Last().Response.Size).IsEqualTo(0);
        source.AddOrUpdate(new Person("c", 3));
        await Assert.That(results.Messages.Last().Response.TotalSize).IsEqualTo(3);
        requests.OnNext(new VirtualRequest(1, 1));
        await Assert.That(results.Data.Items.Single().Name).IsEqualTo("b");
    }
}
