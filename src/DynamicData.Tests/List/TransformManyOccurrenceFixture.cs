namespace DynamicData.Tests.List;

public sealed class TransformManyOccurrenceFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ParentChangeAddsAndRemovesDuplicateChildren(bool refresh)
    {
        using var source = new TestSourceList<Parent<int>>();
        using var results = source.Connect().TransformMany(parent => parent.Children).AsAggregator();
        var parent = new Parent<int>(new[] { 1 });
        source.Add(parent);
        ChangeParent(source, parent, new[] { 1, 1 }, refresh);

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(1);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(0);
        ChangeParent(source, source.Items[0], new[] { 1 }, refresh);

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(0);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(1);
        await Assert.That(results.Messages.Count).IsEqualTo(3);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MixedDuplicateChangesAppendUnmatchedChildrenInIncomingOrder(bool refresh)
    {
        using var source = new TestSourceList<Parent<int>>();
        using var results = source.Connect().TransformMany(parent => parent.Children).AsAggregator();
        var parent = new Parent<int>(new[] { 1, 2, 1 });
        source.AddRange(new[] { parent, new Parent<int>(new[] { 3 }) });
        ChangeParent(source, parent, new[] { 1, 4, 1, 1, 4 }, refresh);

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1, 3, 4, 1, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(3);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(1);
        ChangeParent(source, source.Items[0], new[] { 4, 1, 4, 1, 1 }, refresh);
        await Assert.That(results.Messages.Count).IsEqualTo(2);
        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 1, 1, 3, 4, 1, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task CustomComparerConsumesOccurrencesAndPreservesRepeatedReferences()
    {
        using var source = new TestSourceList<Parent<string>>();
        using var results = source.Connect().TransformMany(parent => parent.Children, StringComparer.OrdinalIgnoreCase).AsAggregator();
        var parent = new Parent<string>(new[] { "A", "a", "B" });
        source.Add(parent);
        source.ReplaceAt(0, new Parent<string>(new[] { "b", "a", "b", "B", "c", "C" }));

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "a", "B", "b", "B", "c", "C" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(4);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task RepeatedReferenceChildOccurrencesAreAddedAndRemovedIndividually()
    {
        using var source = new TestSourceList<Parent<object>>();
        using var results = source.Connect().TransformMany(parent => parent.Children).AsAggregator();
        var child = new object();
        source.Add(new Parent<object>(new[] { child }));
        source.ReplaceAt(0, new Parent<object>(new[] { child, child, child }));
        await Assert.That(results.Data.Count).IsEqualTo(3);
        await Assert.That(results.Messages.Last().Adds).IsEqualTo(2);
        source.ReplaceAt(0, new Parent<object>(new[] { child }));
        await Assert.That(results.Data.Items.Single()).IsSameReferenceAs(child);
        await Assert.That(results.Messages.Last().Removes).IsEqualTo(2);
        await Assert.That(results.Exception).IsNull();
    }

    private static void ChangeParent<T>(TestSourceList<Parent<T>> source, Parent<T> parent, T[] children, bool refresh)
        where T : notnull
    {
        if (refresh)
        {
            parent.Children = children;
            source.Refresh(0);
        }
        else
        {
            source.ReplaceAt(0, new Parent<T>(children));
        }
    }

    private sealed class Parent<T>(T[] children)
        where T : notnull
    {
        public T[] Children { get; set; } = children;
    }
}
