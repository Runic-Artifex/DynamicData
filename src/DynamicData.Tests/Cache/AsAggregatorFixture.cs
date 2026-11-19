using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

/// <summary>
/// DD-048 (fork issue #48, upstream #1209): the specialised aggregators record a source error in
/// <c>Error</c> instead of rethrowing it to whatever raised it.
/// </summary>
public class AsAggregatorFixture
{
    [Test]
    public async Task DistinctAggregatorRecordsTheSourceFailure()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IDistinctChangeSet<int>>();
        using var results = source.AsAggregator();

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the aggregator should record the failure, not rethrow it");
    }

    [Test]
    public async Task PagedAggregatorRecordsTheSourceFailure()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IPagedChangeSet<Person, string>>();
        using var results = source.AsAggregator();

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the aggregator should record the failure, not rethrow it");
    }

    [Test]
    public async Task SortedAggregatorRecordsTheSourceFailure()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<ISortedChangeSet<Person, string>>();
        using var results = source.AsAggregator();

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the aggregator should record the failure, not rethrow it");
    }

    [Test]
    public async Task VirtualAggregatorRecordsTheSourceFailure()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IVirtualChangeSet<Person, string>>();
        using var results = source.AsAggregator();

        var expectedError = new Exception("Test Exception");
        source.OnError(expectedError);

        await Assert.That(results.Error).IsSameReferenceAs(expectedError).Because("the aggregator should record the failure, not rethrow it");
    }
}
