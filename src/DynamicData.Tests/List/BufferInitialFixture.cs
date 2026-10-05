using DynamicData.Tests.Domain;

namespace DynamicData.Tests.List;

public class BufferInitialFixture
{
    private static readonly ICollection<Person> People = Enumerable.Range(1, 10_000).Select(i => new Person(i.ToString(), i)).ToList();

    [Test]
    public async Task InitialChangesPreserveDuplicateOccurrencesAndIndexes()
    {
        var scheduler = new TestScheduler();
        using var source = new SourceList<int>();
        using var results = source.Connect().BufferInitial(TimeSpan.FromSeconds(1), scheduler).AsAggregator();
        source.Add(1);
        source.Add(1);
        source.RemoveAt(1);
        source.Insert(0, 2);
        source.Edit(items => items[1] = 3);
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(results.Messages.Count).IsEqualTo(1);
        await Assert.That(results.Messages[0].Select(change => change.Reason)).IsEquivalentTo(new[] { ListChangeReason.Add, ListChangeReason.Add, ListChangeReason.Remove, ListChangeReason.Add, ListChangeReason.Replace }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 2, 3 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task BufferInitial()
    {
        var scheduler = new TestScheduler();

        using var cache = new SourceList<Person>();
        using var aggregator = cache.Connect().BufferInitial(TimeSpan.FromSeconds(1), scheduler).AsAggregator();
        foreach (var item in People)
        {
            cache.Add(item);
        }

        await Assert.That(aggregator.Data.Count).IsEqualTo(0);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(0);

        scheduler.Start();

        await Assert.That(aggregator.Data.Count).IsEqualTo(10_000);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(1);

        cache.Add(new Person("_New", 1));

        await Assert.That(aggregator.Data.Count).IsEqualTo(10_001);
        await Assert.That(aggregator.Messages.Count).IsEqualTo(2);
    }
}
