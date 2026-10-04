using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Bogus;

using DynamicData.Tests.Utilities;

namespace DynamicData.Tests.Cache;

public partial class ToObservableOptionalFixture
{
    private const int InitialValueSeed = 0x0F710;


    [Test]
    public async Task InitialOptionalNeverFollowsConcurrentSomeWithoutRemoval()
    {
        const int maximumIterations = 20_000;
        const int maximumUnwatchedAdds = 16;
        var value = CreateInitialValue();
        var expected = ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value);
        var randomizer = new Randomizer(InitialValueSeed);
        var unwatchedValues = Enumerable.Range(0, maximumUnwatchedAdds)
            .Select(index => Create($"{value.Key}/{index}", randomizer.AlphaNumeric(16)))
            .ToArray();
        var changeSets = Enumerable.Range(0, maximumUnwatchedAdds + 1)
            .Select(count => new ChangeSet<KeyValuePair, string>(unwatchedValues.Take(count)
                .Select(item => new Change<KeyValuePair, string>(ChangeReason.Add, item.Key, item))
                .Append(new Change<KeyValuePair, string>(ChangeReason.Add, value.Key, value))))
            .ToArray();
        var timeout = TimeSpan.FromSeconds(10);
        using var cancellation = new CancellationTokenSource();
        using var barrier = new Barrier(2);
        IObserver<IChangeSet<KeyValuePair, string>>? sourceObserver = null;
        var completedIterations = 0;
        var invalidSequences = 0;
        var invalidFinalNones = 0;
        string? firstFailure = null;
        Console.WriteLine("Initial optional race: seed={0}, maximumIterations={1}, maximumUnwatchedAdds={2}",
            InitialValueSeed, maximumIterations, maximumUnwatchedAdds);

        // Only the Add races subscription. Vary real changeset work (ignored keys before the watched
        // key), not sleeps or spins. Every input contains exactly one Add for the watched key and no Remove.
        var producer = Task.Factory.StartNew(() =>
        {
            try
            {
                for (var iteration = 0; iteration < maximumIterations; iteration++)
                {
                    Rendezvous(iteration, "subscription started");
                    sourceObserver!.OnNext(changeSets[iteration % changeSets.Length]);
                    Rendezvous(iteration, "value delivered and subscription returned");
                }
            }
            catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
            {
                // The subscriber stops the bounded run immediately after finding a violation.
            }
            catch
            {
                cancellation.Cancel();
                throw;
            }
        }, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        try
        {
            for (var iteration = 0; iteration < maximumIterations; iteration++)
            {
                var source = Observable.Create<IChangeSet<KeyValuePair, string>>(observer =>
                {
                    sourceObserver = observer;
                    Rendezvous(iteration, "subscription started");
                    return Disposable.Empty;
                });
                using var subscription = source
                    .ToObservableOptional(value.Key, initialOptionalWhenMissing: true)
                    .ValidateSynchronization()
                    .RecordValues(out var results);

                Rendezvous(iteration, "value delivered and subscription returned");

                // Both racing calls have returned. Complete only now, on this thread, so the probe
                // does not also race completion against initialization. No further source delivery
                // can occur in this round, and the synchronous terminal notification must be recorded.
                sourceObserver!.OnCompleted();
                completedIterations++;

                var values = results.RecordedValues;
                var validValues = (values.Count == 1 && values[0].Equals(expected))
                    || (values.Count == 2 && !values[0].HasValue && values[1].Equals(expected));
                var validTermination = results.Error is null
                    && results.HasCompleted
                    && results.WhenFinalized.IsCompletedSuccessfully
                    && results.Notifications.Count == values.Count + 1
                    && results.Notifications[^1].Value.Kind is NotificationKind.OnCompleted;
                if (!validValues || !validTermination)
                {
                    invalidSequences++;
                    if (values.Count != 0 && !values[^1].HasValue)
                        invalidFinalNones++;

                    var sequence = string.Join(", ", values.Select(optional => optional.HasValue ? $"Some({optional.Value.Value})" : "None"));
                    firstFailure = $"iteration={iteration}, unwatchedAdds={iteration % changeSets.Length}, values=[{sequence}], completed={results.HasCompleted}, error={results.Error}";
                    break;
                }
            }
        }
        finally
        {
            // There is never an unbounded worker left waiting for a round that the subscriber skipped.
            cancellation.Cancel();
            await producer.WaitAsync(timeout);
            Console.WriteLine("Initial optional race: completedIterations={0}/{1}, invalidSequences={2}, invalidFinalNones={3}, firstFailure={4}",
                completedIterations, maximumIterations, invalidSequences, invalidFinalNones, firstFailure ?? "none");
        }

        await Assert.That(invalidSequences).IsEqualTo(0);

        void Rendezvous(int iteration, string phase)
        {
            if (!barrier.SignalAndWait(timeout, cancellation.Token))
                throw new TimeoutException($"Initial optional race stalled at iteration {iteration}, phase '{phase}'.");
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InitialOptionalPreservesSynchronousEmptyCompletion(bool initialOptionalWhenMissing)
    {
        using var subscription = Observable.Empty<IChangeSet<KeyValuePair, string>>()
            .ToObservableOptional(Key1, initialOptionalWhenMissing)
            .ValidateSynchronization()
            .RecordValues(out var results);

        var expected = initialOptionalWhenMissing
            ? new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None }
            : Array.Empty<ReactiveUI.Primitives.Optional<KeyValuePair>>();
        await Assert.That(results.RecordedValues.SequenceEqual(expected)).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(initialOptionalWhenMissing
                ? new[] { NotificationKind.OnNext, NotificationKind.OnCompleted }
                : new[] { NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task InitialOptionalDoesNotEmitNoneAfterSynchronousError()
    {
        var error = new InvalidOperationException("The source failed during subscription.");
        using var subscription = Observable.Throw<IChangeSet<KeyValuePair, string>>(error)
            .ToObservableOptional(Key1, initialOptionalWhenMissing: true)
            .ValidateSynchronization()
            .RecordValues(out var results);

        await Assert.That(results.RecordedValues).IsEmpty();
        await Assert.That(results.Error).IsSameReferenceAs(error);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnError }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InitialOptionalPreservesSynchronousValuesAndCompletion(bool removeBeforeCompletion)
    {
        var value = CreateInitialValue();
        var subscriptions = 0;
        var disposals = 0;
        var source = Observable.Create<IChangeSet<KeyValuePair, string>>(observer =>
        {
            subscriptions++;
            observer.OnNext(new ChangeSet<KeyValuePair, string> { new(ChangeReason.Add, value.Key, value) });
            if (removeBeforeCompletion)
                observer.OnNext(new ChangeSet<KeyValuePair, string> { new(ChangeReason.Remove, value.Key, value) });
            observer.OnCompleted();
            return Disposable.Create(() => disposals++);
        });
        using var subscription = source
            .ToObservableOptional(value.Key, initialOptionalWhenMissing: true)
            .ValidateSynchronization()
            .RecordValues(out var results);

        var expected = removeBeforeCompletion
            ? new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value), ReactiveUI.Primitives.Optional<KeyValuePair>.None }
            : new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value) };
        await Assert.That(results.RecordedValues.SequenceEqual(expected)).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(removeBeforeCompletion
                ? new[] { NotificationKind.OnNext, NotificationKind.OnNext, NotificationKind.OnCompleted }
                : new[] { NotificationKind.OnNext, NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(subscriptions).IsEqualTo(1);
        await Assert.That(disposals).IsEqualTo(1);
    }

    [Test]
    public async Task InitialOptionalStateIsPerSubscription()
    {
        var value = CreateInitialValue();
        using var source = new TestSourceCache<KeyValuePair, string>(item => item.Key);
        var optional = source.Connect().ToObservableOptional(value.Key, initialOptionalWhenMissing: true);
        source.AddOrUpdate(value);
        using var firstSubscription = optional.ValidateSynchronization().RecordValues(out var firstResults);

        source.RemoveKey(value.Key);
        using var secondSubscription = optional.ValidateSynchronization().RecordValues(out var secondResults);
        await Assert.That(secondResults.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None })).IsTrue();

        source.AddOrUpdate(value);
        firstSubscription.Dispose();
        source.RemoveKey(value.Key);
        source.Complete();

        await Assert.That(firstResults.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value), ReactiveUI.Primitives.Optional<KeyValuePair>.None, ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value) })).IsTrue();
        await Assert.That(firstResults.HasCompleted).IsFalse();
        await Assert.That(firstResults.Error).IsNull();
        await Assert.That(secondResults.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None, ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value), ReactiveUI.Primitives.Optional<KeyValuePair>.None })).IsTrue();
        await Assert.That(secondResults.HasCompleted).IsTrue();
        await Assert.That(secondResults.Error).IsNull();
    }

    [Test]
    public async Task InitialOptionalDisposalDuringInitialNoneReleasesSource()
    {
        var subscriptions = 0;
        var disposals = 0;
        var source = Observable.Create<IChangeSet<KeyValuePair, string>>(_ =>
        {
            subscriptions++;
            return Disposable.Create(() => disposals++);
        });
        using var subscription = source
            .ToObservableOptional(Key1, initialOptionalWhenMissing: true)
            .Take(1)
            .ValidateSynchronization()
            .RecordValues(out var results);

        await Assert.That(results.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None })).IsTrue();
        await Assert.That(results.Error).IsNull();
        await Assert.That(results.HasCompleted).IsTrue();
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(subscriptions).IsEqualTo(1);
        await Assert.That(disposals).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InitialOptionalPropagatesAsynchronousTermination(bool failSource)
    {
        var value = CreateInitialValue();
        var error = new InvalidOperationException("The source failed after subscription.");
        using var source = new ReactiveUI.Primitives.Signals.Signal<IChangeSet<KeyValuePair, string>>();
        using var subscription = source
            .ToObservableOptional(value.Key, initialOptionalWhenMissing: true)
            .ValidateSynchronization()
            .RecordValues(out var results);

        await Assert.That(results.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None })).IsTrue();
        await Assert.That(results.HasCompleted).IsFalse();
        source.OnNext(new ChangeSet<KeyValuePair, string> { new(ChangeReason.Add, value.Key, value) });
        if (failSource)
            source.OnError(error);
        else
            source.OnCompleted();

        await Assert.That(results.RecordedValues.SequenceEqual(new[] { ReactiveUI.Primitives.Optional<KeyValuePair>.None, ReactiveUI.Primitives.Optional<KeyValuePair>.Some(value) })).IsTrue();
        await Assert.That(results.Error).IsSameReferenceAs(failSource ? error : null);
        await Assert.That(results.HasCompleted).IsEqualTo(!failSource);
        await Assert.That(results.Notifications.Select(notification => notification.Value.Kind)).IsEquivalentTo(new[] { NotificationKind.OnNext, NotificationKind.OnNext, failSource ? NotificationKind.OnError : NotificationKind.OnCompleted }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(source.HasObservers).IsFalse();
    }

    private static KeyValuePair CreateInitialValue()
    {
        var randomizer = new Randomizer(InitialValueSeed);
        Console.WriteLine("Initial optional data: seed={0}", InitialValueSeed);
        return Create(randomizer.AlphaNumeric(12), randomizer.AlphaNumeric(16));
    }
}
