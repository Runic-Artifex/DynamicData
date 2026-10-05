// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace DynamicData.Tests.Cache;

public sealed class AsyncDisposeManyIdentityFixture
{
    public enum Termination
    {
        Complete,
        Error,
        Dispose
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EqualDistinctReferences_ReplaceOwnedInstanceAfterForwarding(bool asyncDisposal)
    {
        EqualItem old = asyncDisposal ? new EqualAsyncItem(1) : new EqualDisposableItem(1);
        EqualItem replacement = asyncDisposal ? new EqualAsyncItem(1) : new EqualDisposableItem(1);
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem, int>>();
        ValueRecordingObserver<Unit>? disposals = null;
        var oldDisposalCountDuringUpdate = -1;
        using var subscription = source.AsyncDisposeMany(signal => signal.RecordValues(out disposals))
            .Do(changes =>
            {
                if (changes.Any(change => change.Reason == ChangeReason.Update))
                    oldDisposalCountDuringUpdate = old.DisposalCount;
            })
            .RecordCacheItems(out var results);

        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Add, 1, old) });
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Update, 1, replacement, new ReactiveUI.Primitives.Optional<EqualItem>(old)) });

        await Assert.That(old.Equals(replacement)).IsTrue().Because("equal values must still represent separate owned references");
        await Assert.That(oldDisposalCountDuringUpdate).IsEqualTo(0);
        await Assert.That(old.DisposalCount).IsEqualTo(1);
        await Assert.That(replacement.DisposalCount).IsEqualTo(0);
        await Assert.That(results.RecordedItemsByKey[1]).IsSameReferenceAs(replacement);
        await Assert.That(results.Error).IsNull();

        subscription.Dispose();
        await Assert.That(old.DisposalCount).IsEqualTo(1);
        await Assert.That(replacement.DisposalCount).IsEqualTo(1);

        if (old is EqualAsyncItem oldAsync && replacement is EqualAsyncItem replacementAsync)
        {
            await Assert.That(disposals!.HasCompleted).IsFalse();
            replacementAsync.Complete();
            await Assert.That(disposals.HasCompleted).IsFalse().Because("shutdown still awaits the replaced instance");
            oldAsync.Complete();
        }

        await Assert.That(disposals!.RecordedValues.Count).IsEqualTo(1);
        await Assert.That(disposals.HasCompleted).IsTrue();
        await Assert.That(disposals.Error).IsNull();
    }

    [Test]
    [Arguments(Termination.Complete)]
    [Arguments(Termination.Error)]
    [Arguments(Termination.Dispose)]
    public async Task SameReferenceUpdate_RetainsItemUntilTerminalDisposal(Termination termination)
    {
        var item = new EqualAsyncItem(1);
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem, int>>();
        ValueRecordingObserver<Unit>? disposals = null;
        using var subscription = source.AsyncDisposeMany(signal => signal.RecordValues(out disposals)).RecordCacheItems(out var results);
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Add, 1, item) });
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Update, 1, item, new ReactiveUI.Primitives.Optional<EqualItem>(item)) });
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Refresh, 1, item) });

        await Assert.That(item.DisposalCount).IsEqualTo(0);
        var error = new InvalidOperationException("source");
        switch (termination)
        {
            case Termination.Complete:
                source.OnCompleted();
                break;
            case Termination.Error:
                source.OnError(error);
                break;
            case Termination.Dispose:
                subscription.Dispose();
                break;
        }

        await Assert.That(item.DisposalCount).IsEqualTo(1);
        await Assert.That(results.Error).IsSameReferenceAs(termination == Termination.Error ? error : null);
        await Assert.That(results.HasCompleted).IsEqualTo(termination == Termination.Complete);
        await Assert.That(disposals!.HasCompleted).IsFalse();
        item.Complete();
        await Assert.That(disposals.RecordedValues.Count).IsEqualTo(1);
        await Assert.That(disposals.HasCompleted).IsTrue();
        await Assert.That(disposals.Error).IsNull();
    }

    [Test]
    public async Task EqualReplacement_AsyncDisposalFailureUsesSeparateSignal()
    {
        var old = new EqualAsyncItem(1);
        var replacement = new EqualAsyncItem(1);
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem, int>>();
        ValueRecordingObserver<Unit>? disposals = null;
        using var subscription = source.AsyncDisposeMany(signal => signal.RecordValues(out disposals)).RecordCacheItems(out var results);
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Add, 1, old) });
        source.OnNext(new ChangeSet<EqualItem, int> { new(ChangeReason.Update, 1, replacement, new ReactiveUI.Primitives.Optional<EqualItem>(old)) });

        var sourceError = new InvalidOperationException("source");
        var disposalError = new InvalidOperationException("replaced item disposal");
        source.OnError(sourceError);
        old.Fail(disposalError);
        replacement.Complete();

        await Assert.That(results.Error).IsSameReferenceAs(sourceError);
        await Assert.That(disposals!.Error).IsSameReferenceAs(disposalError);
        await Assert.That(disposals.RecordedValues).IsEmpty();
        await Assert.That(disposals.HasCompleted).IsFalse();
        await Assert.That(old.DisposalCount).IsEqualTo(1);
        await Assert.That(replacement.DisposalCount).IsEqualTo(1);
    }

    [Test]
    public async Task ValueItems_EqualUpdateRetainsValueAndChangedUpdateDisposesPrevious()
    {
        var disposedVersions = new List<int>();
        var old = new DisposableValue(1, disposedVersions);
        var equal = new DisposableValue(1, disposedVersions);
        var replacement = new DisposableValue(2, disposedVersions);
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<DisposableValue, int>>();
        ValueRecordingObserver<Unit>? disposals = null;
        using var subscription = source.AsyncDisposeMany(signal => signal.RecordValues(out disposals)).RecordCacheItems(out var results);
        source.OnNext(new ChangeSet<DisposableValue, int> { new(ChangeReason.Add, 1, old) });
        source.OnNext(new ChangeSet<DisposableValue, int> { new(ChangeReason.Update, 1, equal, new ReactiveUI.Primitives.Optional<DisposableValue>(old)) });
        await Assert.That(disposedVersions).IsEmpty();

        source.OnNext(new ChangeSet<DisposableValue, int> { new(ChangeReason.Update, 1, replacement, new ReactiveUI.Primitives.Optional<DisposableValue>(equal)) });
        await Assert.That(disposedVersions).IsEquivalentTo(new[] { 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        source.OnCompleted();

        await Assert.That(disposedVersions).IsEquivalentTo(new[] { 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(disposals!.HasCompleted).IsTrue();
    }

    private abstract class EqualItem(int id)
    {
        public int Id { get; } = id;

        public int DisposalCount { get; protected set; }

        public override bool Equals(object? obj) => obj is EqualItem other && Id == other.Id;

        public override int GetHashCode() => Id;
    }

    private sealed class EqualDisposableItem(int id) : EqualItem(id), IDisposable
    {
        public void Dispose() => DisposalCount++;
    }

    private sealed class EqualAsyncItem(int id) : EqualItem(id), IAsyncDisposable
    {
        private readonly TaskCompletionSource _completion = new();

        public ValueTask DisposeAsync()
        {
            DisposalCount++;
            return new ValueTask(_completion.Task);
        }

        public void Complete() => _completion.SetResult();

        public void Fail(Exception error) => _completion.SetException(error);
    }

    private readonly record struct DisposableValue(int Version, List<int> DisposedVersions) : IDisposable
    {
        public void Dispose() => DisposedVersions.Add(Version);
    }
}
