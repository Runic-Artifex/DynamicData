using ReevaluatorSignal = ReactiveUI.Primitives.Signals.Signal<int>;

namespace DynamicData.Tests.List;

/// <summary>
/// Pins the list re-evaluator error and completion policy, which matches the cache operator:
/// a re-evaluator error ends only that item's refresh subscription.
/// </summary>
public class AutoRefreshOnObservableErrorPolicyFixture
{
    [Test]
    public async Task ReevaluatorFails_StreamContinuesAndOtherItemsRefresh()
    {
        using var source = new TestSourceList<string>();
        var reevaluators = new Dictionary<string, ReevaluatorSignal>
        {
            ["a"] = new(),
            ["b"] = new(),
            ["c"] = new(),
        };
        source.AddRange(["a", "b"]);

        using var subscription = source.Connect()
            .AutoRefreshOnObservable(item => reevaluators[item])
            .ValidateSynchronization()
            .RecordListItems(out var results);

        reevaluators["a"].OnError(new InvalidOperationException("Test"));

        await Assert.That(results.Error).IsNull().Because("a re-evaluator error ends only that item's refresh subscription");
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(reevaluators["a"].HasObservers).IsFalse();

        reevaluators["b"].OnNext(1);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2);
        var refresh = results.RecordedChangeSets[1].Single();
        await Assert.That(refresh.Reason).IsEqualTo(ListChangeReason.Refresh);
        await Assert.That(refresh.Item.Current).IsEqualTo("b");
        await Assert.That(refresh.Item.CurrentIndex).IsEqualTo(1);

        source.Add("c");
        reevaluators["c"].OnNext(1);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(4).Because("the source and new items continue after a re-evaluator error");
        await Assert.That(results.RecordedChangeSets[3].Single().Item.Current).IsEqualTo("c");
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { "a", "b", "c" });
    }

    [Test]
    public async Task SourceCompletes_CompletionWaitsForReevaluatorsToEndByCompletionOrError()
    {
        using var source = new TestSourceList<string>();
        var first = new ReevaluatorSignal();
        var second = new ReevaluatorSignal();
        source.AddRange(["a", "b"]);

        using var subscription = source.Connect()
            .AutoRefreshOnObservable(item => item == "a" ? first : second)
            .ValidateSynchronization()
            .RecordListItems(out var results);

        source.Complete();
        await Assert.That(results.HasCompleted).IsFalse().Because("re-evaluators are still active");

        first.OnError(new InvalidOperationException("Test"));
        await Assert.That(results.HasCompleted).IsFalse().Because("one re-evaluator is still active");

        second.OnNext(1);
        await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(2).Because("remaining items still refresh after the source completed");

        second.OnCompleted();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue().Because("the source completed and every re-evaluator ended");
    }

    [Test]
    public async Task SourceFails_ErrorPropagates()
    {
        using var source = new TestSourceList<string>();
        var reevaluator = new ReevaluatorSignal();
        source.Add("a");

        using var subscription = source.Connect()
            .AutoRefreshOnObservable(_ => reevaluator)
            .ValidateSynchronization()
            .RecordListItems(out var results);

        var error = new InvalidOperationException("Test");
        source.SetError(error);

        await Assert.That(results.Error).IsSameReferenceAs(error);
    }
}
