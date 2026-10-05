using ReactiveUI.Primitives.Signals;
namespace DynamicData.Tests.List;

public sealed class VirtualisationContractFixture
{
    [Test]
    public async Task ZeroAndInvalidRequestsPreserveTheLastValidWindow()
    {
        using var source = new SourceList<int>();
        source.AddRange(Enumerable.Range(0, 10));
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(4, 0));
        var messages = new List<IVirtualChangeSet<int>>();
        var rows = new List<int>();
        using var subscription = source.Connect().Virtualise(requests).Do(messages.Add).Clone(rows).Subscribe();
        await Assert.That(rows.Count).IsEqualTo(0);
        await Assert.That(messages.All(batch => batch.Count == 0)).IsTrue();
        await Assert.That(messages.Last().Response.TotalSize).IsEqualTo(10);
        requests.OnNext(new VirtualRequest(-1, 2));
        requests.OnNext(new VirtualRequest(4, -1));
        requests.OnNext(null!);
        source.Add(10);
        await Assert.That(messages.Last().Response.Size).IsEqualTo(0);
        await Assert.That(messages.Last().Response.StartIndex).IsEqualTo(4);
        await Assert.That(messages.Last().Response.TotalSize).IsEqualTo(11);
        requests.OnNext(new VirtualRequest(4, 2));
        await Assert.That(rows).IsEquivalentTo(new[] { 4, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        requests.OnNext(new VirtualRequest(4, 0));
        await Assert.That(rows.Count).IsEqualTo(0);
    }

    [Test]
    public async Task EqualOccurrencesKeepTheirPositionsDuringIndexedUpdatesAndMoves()
    {
        using var source = new SourceList<string>();
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(1, 3));
        var rows = new List<string>();
        using var subscription = source.Connect().Virtualise(requests).Clone(rows).Subscribe();
        source.AddRange(new[] { "x", "same", "same", "z", "same" });
        await Assert.That(rows).IsEquivalentTo(new[] { "same", "same", "z" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.Move(3, 1);
        await Assert.That(rows).IsEquivalentTo(new[] { "z", "same", "same" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.RemoveAt(2);
        await Assert.That(rows).IsEquivalentTo(new[] { "z", "same", "same" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.ReplaceAt(2, "new");
        await Assert.That(rows).IsEquivalentTo(new[] { "z", "new", "same" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        requests.OnNext(new VirtualRequest(4, 3));
        await Assert.That(rows.Count).IsEqualTo(0);
        source.Insert(0, "first");
        await Assert.That(rows).IsEquivalentTo(new[] { "same" });
    }

    [Test]
    public async Task DuplicateRefreshForwardsTheCorrectVisibleIndex()
    {
        using var source = new Signal<IChangeSet<int>>();
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(1, 2));
        var messages = new List<IVirtualChangeSet<int>>();
        using var subscription = source.Virtualise(requests).Subscribe(messages.Add);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.AddRange, new[] { 1, 1, 1 }, 0) });
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Refresh, 1, 2) });
        await Assert.That(messages.Last().Single().Reason).IsEqualTo(ListChangeReason.Refresh);
        await Assert.That(messages.Last().Single().Item.CurrentIndex).IsEqualTo(1);
    }

    [Test]
    public async Task RandomizedDuplicateReplayMatchesTheRequestedSourceSlice()
    {
        using var source = new SourceList<int>();
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 4));
        var rows = new List<int>();
        var messages = new List<IVirtualChangeSet<int>>();
        using var subscription = source.Connect().Virtualise(requests).Do(messages.Add).Clone(rows).Subscribe();
        var expected = new List<int>();
        var random = new Random(1037);
        var start = 0;
        var size = 4;
        for (var operation = 0; operation < 250; operation++)
        {
            switch (random.Next(5))
            {
                case 0:
                    var index = random.Next(expected.Count + 1);
                    var value = random.Next(3);
                    expected.Insert(index, value);
                    source.Insert(index, value);
                    break;
                case 1 when expected.Count != 0:
                    index = random.Next(expected.Count);
                    expected.RemoveAt(index);
                    source.RemoveAt(index);
                    break;
                case 2 when expected.Count != 0:
                    index = random.Next(expected.Count);
                    var destination = random.Next(expected.Count);
                    value = expected[index];
                    expected.RemoveAt(index);
                    expected.Insert(destination, value);
                    source.Move(index, destination);
                    break;
                case 3 when expected.Count != 0:
                    index = random.Next(expected.Count);
                    value = random.Next(3);
                    expected[index] = value;
                    source.ReplaceAt(index, value);
                    break;
                default:
                    start = random.Next(expected.Count + 3);
                    size = random.Next(5);
                    requests.OnNext(new VirtualRequest(start, size));
                    break;
            }
            await Assert.That(rows).IsEquivalentTo(expected.Skip(start).Take(size), TUnit.Assertions.Enums.CollectionOrdering.Matching);
            if (messages.Count != 0)
            {
                await Assert.That(messages.Last().Response.TotalSize).IsEqualTo(expected.Count);
                await Assert.That(messages.Last().Response.Size).IsEqualTo(size);
            }
        }
    }

    [Test]
    public async Task EmptyViewportForwardsCompletionAndErrors()
    {
        using var requests = new StateSignal<IVirtualRequest>(new VirtualRequest(0, 0));
        var completed = false;
        using var source = new Signal<IChangeSet<int>>();
        using var subscription = source.Virtualise(requests).Subscribe(_ => { }, () => completed = true);
        source.OnCompleted();
        await Assert.That(completed).IsFalse();
        requests.OnCompleted();
        await Assert.That(completed).IsTrue();

        var failure = new InvalidOperationException("failure");
        Exception? actual = null;
        using var errors = Observable.Throw<IChangeSet<int>>(failure).Virtualise(Observable.Never<IVirtualRequest>()).Subscribe(_ => { }, error => actual = error);
        await Assert.That(actual).IsSameReferenceAs(failure);
    }
}
