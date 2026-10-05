namespace DynamicData.Tests.List;

public sealed class EditDiffOccurrenceFixture
{
    [Test]
    public async Task DuplicateCountsAreAmendedWithoutReorderingRetainedItems()
    {
        using var source = new SourceList<int>();
        using var results = source.Connect().AsAggregator();
        source.AddRange(new[] { 2, 1, 2, 3, 1 });
        source.EditDiff(new[] { 1, 2, 1, 4, 4, 1 });

        await Assert.That(source.Items).IsEquivalentTo(new[] { 2, 1, 1, 4, 4, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Data.Items).IsEquivalentTo(source.Items, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(2);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(3);

        source.EditDiff(new[] { 4, 1, 1, 2, 4, 1 });
        await Assert.That(source.Items).IsEquivalentTo(new[] { 2, 1, 1, 4, 4, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Count).IsEqualTo(2);
    }

    [Test]
    public async Task CustomComparerKeepsOriginalReferencesAndAppendsUnmatchedIncomingOccurrences()
    {
        using var source = new SourceList<string>();
        using var results = source.Connect().AsAggregator();
        source.AddRange(new[] { "A", "a", "B" });
        source.EditDiff(new[] { "b", "a", "b", "B", "c", "C" }, StringComparer.OrdinalIgnoreCase);

        await Assert.That(source.Items).IsEquivalentTo(new[] { "A", "B", "b", "B", "c", "C" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(1);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(4);
        await Assert.That(results.Data.Items).IsEquivalentTo(source.Items, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task EmptySourceAddsEveryOccurrenceAndEmptyTargetRemovesEveryOccurrence()
    {
        using var source = new SourceList<int>();
        using var results = source.Connect().AsAggregator();
        source.EditDiff(new[] { 1, 1, 2, 2, 1 });
        await Assert.That(source.Items).IsEquivalentTo(new[] { 1, 1, 2, 2, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(5);

        source.EditDiff(Array.Empty<int>());
        await Assert.That(source.Items).IsEmpty();
        await Assert.That(results.Data.Items).IsEmpty();
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(5);
    }

    [Test]
    public async Task SelfItemsAreMaterializedBeforeEditingAndUnchangedDuplicatesEmitNoChanges()
    {
        using var source = new SourceList<int>();
        using var results = source.Connect().AsAggregator();
        source.AddRange(new[] { 1, 2, 1 });
        source.EditDiff(source.Items);
        source.EditDiff(source.Items.Where(static item => item == 1));

        await Assert.That(source.Items).IsEquivalentTo(new[] { 1, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Count).IsEqualTo(2);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(1);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(0);
    }
}
