using System;
using System.Collections.Generic;
using System.Linq;
using ReactiveUI.Primitives.Signals;
#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.Cache;

public class RemoveKeyIdentityFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EqualValuesRefreshTheCorrectOccurrence(bool initialSnapshot)
    {
        using var source = new SourceCache<EqualItem, int>(item => item.Key);
        var first = new EqualItem(1, 7) { Included = false };
        var second = new EqualItem(2, 7);
        if (initialSnapshot)
            source.AddOrUpdate(new[] { first, second });
        using var subscription = source.Connect().RemoveKey().Filter(item => item.Included)
            .ValidateChangeSets().RecordListItems(out var results);
        if (!initialSnapshot)
            source.AddOrUpdate(new[] { first, second });
        await Assert.That(results.RecordedItems.Single()).IsSameReferenceAs(second);
        second.Included = false;
        source.Refresh(second);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Count).IsEqualTo(0);
    }

    [Test]
    public async Task RemovingAndUpdatingSecondEqualReferenceKeepsFirstReference()
    {
        using var source = new SourceCache<EqualItem, int>(item => item.Key);
        using var subscription = source.Connect().RemoveKey().ValidateChangeSets()
            .Bind(out var collection).Subscribe();
        var first = new EqualItem(1, 7);
        var second = new EqualItem(2, 7);
        source.AddOrUpdate(new[] { first, second });
        source.RemoveKey(2);
        await Assert.That(collection.Single()).IsSameReferenceAs(first);
        source.AddOrUpdate(second);
        var replacement = new EqualItem(2, 8);
        source.AddOrUpdate(replacement);
        await Assert.That(collection[0]).IsSameReferenceAs(first);
        await Assert.That(collection[1]).IsSameReferenceAs(replacement);
    }

    [Test]
    public async Task SharedReferenceRefreshesBothKeysAndRemovalDoesNotLoseTheSurvivor()
    {
        using var source = new SourceCache<KeyValuePair<int, EqualItem>, int>(item => item.Key);
        var shared = new EqualItem(0, 7);
        var first = new KeyValuePair<int, EqualItem>(1, shared);
        var second = new KeyValuePair<int, EqualItem>(2, shared);
        using var subscription = source.Connect().Transform(item => item.Value).RemoveKey()
            .Filter(item => item.Included).ValidateChangeSets().RecordListItems(out var results);
        source.AddOrUpdate(new[] { first, second });
        shared.Included = false;
        source.Refresh(new[] { first, second });
        await Assert.That(results.RecordedItems.Count).IsEqualTo(0);
        shared.Included = true;
        source.Refresh(new[] { first, second });
        source.RemoveKey(2);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Single()).IsSameReferenceAs(shared);
        source.RemoveKey(1);
        await Assert.That(results.RecordedItems.Count).IsEqualTo(0);
    }

    [Test]
    public async Task EqualStructOccurrencesKeepTheirKeyDuringRefreshUpdateAndRemove()
    {
        using var source = new SourceCache<EqualValue, int>(item => item.Key);
        var included = new HashSet<int> { 2 };
        var first = new EqualValue(1, 7);
        var second = new EqualValue(2, 7);
        using var subscription = source.Connect().RemoveKey().Filter(item => included.Contains(item.Key))
            .ValidateChangeSets().RecordListItems(out var results);
        source.AddOrUpdate(new[] { first, second });
        await Assert.That(results.RecordedItems.Single().Key).IsEqualTo(2);
        included.Clear();
        source.Refresh(second);
        await Assert.That(results.RecordedItems.Count).IsEqualTo(0);
        included.UnionWith(new[] { 1, 2 });
        source.Refresh(new[] { first, second });
        source.AddOrUpdate(new EqualValue(2, 8));
        source.RemoveKey(2);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Single().Key).IsEqualTo(1);
    }

    [Test]
    public async Task IndexedOperationsAndUnindexedUpdatesReportSequentialActualPositions()
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var subscription = source.RemoveKey().ValidateChangeSets().RecordListItems(out var results);
        source.OnNext(new ChangeSet<int, int>
        {
            new(ChangeReason.Add, 1, 11), new(ChangeReason.Add, 2, 22), new(ChangeReason.Add, 3, 33, 0)
        });
        source.OnNext(new ChangeSet<int, int> { new(2, 22, 0, 2) });
        source.OnNext(new ChangeSet<int, int>
        {
            new(ChangeReason.Update, 3, 34, 33, 2, 1),
            new(ChangeReason.Refresh, 2, 22),
            new(ChangeReason.Remove, 1, 11, 1)
        });
        source.OnNext(new ChangeSet<int, int> { new(ChangeReason.Update, 2, 23, new ReactiveUI.Primitives.Optional<int>(22)) });
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 34, 23 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        var metadata = results.RecordedChangeSets.SelectMany(changes => changes)
            .Select(change => (change.Reason, change.Item.CurrentIndex, change.Item.PreviousIndex));
        await Assert.That(metadata).IsEquivalentTo(new[]
        {
            (ListChangeReason.Add, 0, -1), (ListChangeReason.Add, 1, -1), (ListChangeReason.Add, 0, -1),
            (ListChangeReason.Moved, 0, 2), (ListChangeReason.Remove, 1, -1), (ListChangeReason.Add, 2, -1),
            (ListChangeReason.Replace, 0, 0), (ListChangeReason.Remove, 1, -1),
            (ListChangeReason.Remove, 0, -1), (ListChangeReason.Add, 1, -1)
        }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task SortedCacheReordersThenRefreshesAndRemovesByKey()
    {
        using var source = new SourceCache<EqualItem, int>(item => item.Key);
        using var subscription = source.Connect().Sort(Comparer<EqualItem>.Create((left, right) => left.Value.CompareTo(right.Value)))
            .RemoveKey().ValidateChangeSets().RecordListItems(out var results);
        var first = new EqualItem(1, 3);
        var second = new EqualItem(2, 1);
        var third = new EqualItem(3, 2);
        source.AddOrUpdate(new[] { first, second, third });
        second.Value = 4;
        source.Refresh(second);
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems.Select(item => item.Key)).IsEquivalentTo(new[] { 3, 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.RemoveKey(2);
        await Assert.That(results.RecordedItems.Select(item => item.Key)).IsEquivalentTo(new[] { 3, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task SubscribersAndResubscriptionsHaveIndependentPositions()
    {
        using var source = new SourceCache<int, int>(item => item);
        var projection = source.Connect().RemoveKey();
        using var first = projection.ValidateChangeSets().RecordListItems(out var firstResults);
        source.AddOrUpdate(new[] { 1, 2, 3 });
        source.AddOrUpdate(1); // Unindexed update moves this subscription's entry to the end.
        using var second = projection.ValidateChangeSets().RecordListItems(out var secondResults);
        source.Refresh(1);
        await Assert.That(firstResults.RecordedChangeSets[^1].Single().Item.CurrentIndex).IsEqualTo(2);
        await Assert.That(secondResults.RecordedChangeSets[^1].Single().Item.CurrentIndex).IsEqualTo(0);
        first.Dispose();
        source.RemoveKey(2);
        using var resubscribed = projection.ValidateChangeSets().RecordListItems(out var thirdResults);
        source.Refresh(1);
        await Assert.That(thirdResults.RecordedChangeSets[^1].Single().Item.CurrentIndex).IsEqualTo(0);
        await Assert.That(firstResults.Error).IsNull();
        await Assert.That(secondResults.Error).IsNull();
        await Assert.That(thirdResults.Error).IsNull();
    }

    [Test]
    public async Task TrackedPositionsOverrideStaleMetadataAndInvalidDestinationsAppend()
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var subscription = source.RemoveKey().ValidateChangeSets().RecordListItems(out var results);
        source.OnNext(new ChangeSet<int, int>
        {
            new(ChangeReason.Add, 1, 11, 10), new(ChangeReason.Add, 2, 22, -2), new(ChangeReason.Add, 3, 33)
        });
        source.OnNext(new ChangeSet<int, int>
        {
            new(1, 11, 99, 2), // Actual previous index is zero; the destination is outside the projection.
            new(ChangeReason.Remove, 2, 22, 99),
            new(ChangeReason.Update, 3, 34, new ReactiveUI.Primitives.Optional<int>(33), 99, 99)
        });
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedItems).IsEquivalentTo(new[] { 11, 34 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.RecordedChangeSets[^1].Select(change => (change.Item.CurrentIndex, change.Item.PreviousIndex)))
            .IsEquivalentTo(new[] { (2, 0), (0, -1), (0, -1), (1, -1) }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task UnknownChangesDoNotThrowOrAlterOtherTrackedPositions()
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var subscription = source.RemoveKey().RecordValues(out var results);
        source.OnNext(new ChangeSet<int, int> { new(ChangeReason.Add, 1, 11), new(ChangeReason.Add, 2, 22) });
        source.OnNext(new ChangeSet<int, int>
        {
            new(ChangeReason.Refresh, 9, 99), new(ChangeReason.Remove, 9, 99, 0),
            new(ChangeReason.Update, 9, 100, 99, 0, 0), new(9, 100, 0, 0), new(ChangeReason.Add, 1, 11)
        });
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues[^1].Select(change => change.Item.CurrentIndex))
            .IsEquivalentTo(Enumerable.Repeat(-1, 7), TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.OnNext(new ChangeSet<int, int> { new(ChangeReason.Refresh, 2, 22) });
        await Assert.That(results.RecordedValues[^1].Single().Item.CurrentIndex).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TerminalNotificationsReleaseSourceAndDisposedSubscriptionsStopReceiving(bool error)
    {
        using var source = new Signal<IChangeSet<int, int>>();
        using var subscription = source.RemoveKey().RecordValues(out var results);
        source.OnNext(new ChangeSet<int, int> { new(ChangeReason.Add, 1, 11) });
        var failure = new InvalidOperationException("source failure");
        if (error)
            source.OnError(failure);
        else
            source.OnCompleted();
        await Assert.That(results.HasCompleted).IsEqualTo(!error);
        await Assert.That(results.Error).IsSameReferenceAs(error ? failure : null);
        await Assert.That(source.HasObservers).IsFalse();
        using var live = new Signal<IChangeSet<int, int>>();
        using var disposed = live.RemoveKey().RecordValues(out var disposedResults);
        disposed.Dispose();
        live.OnNext(new ChangeSet<int, int> { new(ChangeReason.Add, 1, 11) });
        await Assert.That(disposedResults.RecordedValues.Count).IsEqualTo(0);
        await Assert.That(live.HasObservers).IsFalse();
    }

    [Test]
    [Arguments(1182)]
    [Arguments(1192)]
    public async Task RandomizedBatchedReplayMatchesKeyedReferenceAfterEveryBatch(int seed)
    {
        using var source = new Signal<IChangeSet<EqualItem, int>>();
        using var subscription = source.RemoveKey().ValidateChangeSets().RecordListItems(out var results);
        var random = new Random(seed);
        var expected = new List<EqualItem>();
        var nextKey = 0;
        for (var batch = 0; batch < 1_000; ++batch)
        {
            var changes = new ChangeSet<EqualItem, int>();
            for (var operation = 0; operation < random.Next(1, 9); ++operation)
            {
                var reason = expected.Count == 0 ? 0 : random.Next(5);
                var index = expected.Count == 0 ? 0 : random.Next(expected.Count);
                switch (reason)
                {
                    case 0:
                        var added = new EqualItem(nextKey++, random.Next(3));
                        var addIndex = random.Next(expected.Count + 1);
                        var indexedAdd = random.Next(2) == 0;
                        changes.Add(new(ChangeReason.Add, added.Key, added, indexedAdd ? addIndex : -1));
                        expected.Insert(indexedAdd ? addIndex : expected.Count, added);
                        break;
                    case 1:
                        var removed = expected[index];
                        changes.Add(new(ChangeReason.Remove, removed.Key, removed, random.Next(2) == 0 ? index : -1));
                        expected.RemoveAt(index);
                        break;
                    case 2:
                        var previous = expected[index];
                        var updated = new EqualItem(previous.Key, random.Next(3));
                        expected.RemoveAt(index);
                        var updateIndex = random.Next(expected.Count + 1);
                        var indexedUpdate = random.Next(2) == 0;
                        changes.Add(new(ChangeReason.Update, previous.Key, updated, previous, indexedUpdate ? updateIndex : -1, indexedUpdate ? index : -1));
                        expected.Insert(indexedUpdate ? updateIndex : expected.Count, updated);
                        break;
                    case 3:
                        changes.Add(new(ChangeReason.Refresh, expected[index].Key, expected[index]));
                        break;
                    case 4:
                        var moved = expected[index];
                        expected.RemoveAt(index);
                        var moveIndex = random.Next(expected.Count + 1);
                        changes.Add(new(moved.Key, moved, moveIndex, index));
                        expected.Insert(moveIndex, moved);
                        break;
                }
            }

            source.OnNext(changes);
            await Assert.That(results.Error).IsNull();
            // Keys and references, rather than value equality, are the independent occurrence oracle.
            await Assert.That(results.RecordedItems.Select(item => item.Key)).IsEquivalentTo(expected.Select(item => item.Key), TUnit.Assertions.Enums.CollectionOrdering.Matching);
            await Assert.That(results.RecordedItems.Zip(expected, ReferenceEquals).All(same => same)).IsTrue();
        }
    }

    private sealed class EqualItem(int key, int value)
    {
        public int Key { get; } = key;
        public int Value { get; set; } = value;
        public bool Included { get; set; } = true;
        public override bool Equals(object? other) => other is EqualItem item && Value == item.Value;
        public override int GetHashCode() => Value;
    }

    private readonly struct EqualValue(int key, int value) : IEquatable<EqualValue>
    {
        public int Key { get; } = key;
        public int Value { get; } = value;
        public bool Equals(EqualValue other) => Value == other.Value;
        public override bool Equals(object? other) => other is EqualValue item && Equals(item);
        public override int GetHashCode() => Value;
    }
}
