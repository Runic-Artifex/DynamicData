namespace DynamicData.Tests.List;

public class QueryTerminalFixture
{
    [Test]
    public async Task FiniteQueryEmitsItsValueBeforeCompleting()
    {
        var events = new List<string>();
        using var subscription = Observable.Return<IChangeSet<int>>(new ChangeSet<int> { new(ListChangeReason.Add, 42, 0) })
            .QueryWhenChanged(values => values.Single())
            .Subscribe(value => events.Add($"next:{value}"), _ => events.Add("error"), () => events.Add("complete"));
        await Assert.That(events).IsEquivalentTo(new[] { "next:42", "complete" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task SourceAndSelectorErrorsReachTheQueryObserver()
    {
        var expected = new InvalidOperationException("query");
        Exception? sourceError = null;
        Exception? selectorError = null;
        using var failedSource = Observable.Throw<IChangeSet<int>>(expected).QueryWhenChanged().Subscribe(_ => { }, error => sourceError = error);
        using var failedSelector = Observable.Return<IChangeSet<int>>(new ChangeSet<int>())
            .QueryWhenChanged<int, int>(_ => throw expected).Subscribe(_ => { }, error => selectorError = error);
        await Assert.That(sourceError).IsSameReferenceAs(expected);
        await Assert.That(selectorError).IsSameReferenceAs(expected);
    }
}
