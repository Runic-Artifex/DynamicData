namespace DynamicData.Tests.List;

public sealed class TransformAsyncOccurrenceFixture
{
    [Test]
    public async Task IndexlessValueRemovalsAndAddsInOneBatchPreserveOrderAndMultiplicity()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        using var results = source.TransformAsync<int, int>(Task.FromResult).AsAggregator();

        source.OnNext(new ChangeSet<int>
        {
            new(ListChangeReason.AddRange, new[] { 1, 2, 1, 3, 2 })
        });
        source.OnNext(new ChangeSet<int>
        {
            new(ListChangeReason.Remove, 1),
            new(ListChangeReason.Add, 4, 1),
            new(ListChangeReason.RemoveRange, new[] { 2, 1 }),
            new(ListChangeReason.AddRange, new[] { 5, 5 })
        });
        source.OnCompleted();

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { 4, 3, 2, 5, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(3);
        await Assert.That(results.Messages[1].Adds).IsEqualTo(3);
        await Assert.That(results.Exception).IsNull();
        await Assert.That(results.IsCompleted).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IndexlessRemovalPrefersReferenceIdentityOverEqualSources(bool range)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.TransformAsync<EqualItem, string>(item => Task.FromResult(item.Name)).AsAggregator();
        var first = new EqualItem(1, "first");
        var second = new EqualItem(1, "second");
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { first, second })
        });
        source.OnNext(new ChangeSet<EqualItem>
        {
            range ? new(ListChangeReason.RemoveRange, new[] { second }) : new(ListChangeReason.Remove, second)
        });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "first" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task IndexlessRangeConsumesExactlyOneOccurrencePerRepeatedReference()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.TransformAsync<EqualItem, string>((EqualItem item, int index) => Task.FromResult($"{item.Name}:{index}")).AsAggregator();
        var repeated = new EqualItem(1, "repeated");
        var other = new EqualItem(2, "other");
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { repeated, other, repeated, repeated })
        });
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.RemoveRange, new[] { repeated, repeated })
        });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "other:1", "repeated:3" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(2);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IndexlessRemovalFallsBackToSourceEqualityAndIgnoresMissingValues(bool range)
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<EqualItem>>();
        using var results = source.TransformAsync<EqualItem, string>(item => Task.FromResult(item.Name)).AsAggregator();
        source.OnNext(new ChangeSet<EqualItem>
        {
            new(ListChangeReason.AddRange, new[] { new EqualItem(1, "first"), new EqualItem(1, "second"), new EqualItem(2, "other") })
        });
        var equal = new EqualItem(1, "equal removal");
        var missing = new EqualItem(3, "missing");
        source.OnNext(range
            ? new ChangeSet<EqualItem> { new(ListChangeReason.RemoveRange, new[] { equal, missing }) }
            : new ChangeSet<EqualItem> { new(ListChangeReason.Remove, equal), new(ListChangeReason.Remove, missing) });

        await Assert.That(results.Data.Items).IsEquivalentTo(new[] { "second", "other" }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Messages[1].Removes).IsEqualTo(1);
        await Assert.That(results.Exception).IsNull();
    }

    [Test]
    public async Task AwaitedTransformProcessesQueuedRemovalBeforeCompletion()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        // Inline task continuations make SetResult a barrier for the entire queued transform.
        var release = new TaskCompletionSource<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var messages = new List<IChangeSet<int>>();
        using var subscription = source.TransformAsync<int, int>(value => value == 1 ? release.Task : Task.FromResult(value))
            .Subscribe(messages.Add, completed.SetException, () => completed.SetResult());

        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1) });
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Remove, 1), new(ListChangeReason.Add, 2) });
        source.OnCompleted();
        await Assert.That(messages).IsEmpty();
        await Assert.That(completed.Task.IsCompleted).IsFalse();
        release.SetResult(1);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.That(messages.Count).IsEqualTo(2);
        await Assert.That(messages[1].Removes).IsEqualTo(1);
        await Assert.That(messages[1].Adds).IsEqualTo(1);
        await Assert.That(messages[1].First().Item.Current).IsEqualTo(1);
        await Assert.That(messages[1].Last().Item.Current).IsEqualTo(2);
    }

    [Test]
    public async Task DisposalCancelsAwaitedTransformAndSuppressesLateResultsAndQueuedFactories()
    {
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<int>>();
        // Complete the deliberately cancellation-ignoring factory inline after disposal.
        var release = new TaskCompletionSource<int>();
        var factoryCalls = 0;
        var notifications = 0;
        CancellationToken token = default;
        using var subscription = source.TransformAsync<int, int>((value, _, _, cancel) =>
            {
                factoryCalls++;
                token = cancel;
                return release.Task;
            })
            .Subscribe(_ => notifications++, _ => notifications++, () => notifications++);
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Add, 1) });
        source.OnNext(new ChangeSet<int> { new(ListChangeReason.Remove, 1), new(ListChangeReason.Add, 2) });

        await Assert.That(factoryCalls).IsEqualTo(1);
        subscription.Dispose();
        await Assert.That(token.IsCancellationRequested).IsTrue();
        release.SetResult(1);
        await Assert.That(factoryCalls).IsEqualTo(1);
        await Assert.That(notifications).IsEqualTo(0);
    }

    private sealed class EqualItem(int key, string name) : IEquatable<EqualItem>
    {
        public string Name { get; } = name;

        private int Key { get; } = key;

        public bool Equals(EqualItem? other) => other?.Key == Key;

        public override bool Equals(object? obj) => obj is EqualItem other && Equals(other);

        public override int GetHashCode() => Key;
    }
}
