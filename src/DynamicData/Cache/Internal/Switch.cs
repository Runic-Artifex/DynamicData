// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM

namespace DynamicData.Reactive.Cache.Internal;
#else
namespace DynamicData.Cache.Internal;
#endif

internal sealed class Switch<TObject, TKey>(IObservable<IObservable<IChangeSet<TObject, TKey>>> sources)
    where TObject : notnull
    where TKey : notnull
{
    private readonly IObservable<IObservable<IChangeSet<TObject, TKey>>> _sources = sources ?? throw new ArgumentNullException(nameof(sources));

    public IObservable<IChangeSet<TObject, TKey>> Run() => Observable.Create<IChangeSet<TObject, TKey>>(observer =>
    {
        // Switching is done by hand rather than with Observable.Switch, which holds its gate across the
        // whole of downstream delivery. Here the gate is never held around user code, which can loop back
        // into this operator, so state read before such a call may be stale after it and every step
        // re-checks that its generation is still current.
        var queue = new DeliveryQueue<IChangeSet<TObject, TKey>>(observer);

        // What has been published downstream, so that switching away can take it back out.
        var current = new Cache<TObject, TKey>();

        // The activation whose removal of the previous source's items is still unpublished. The removals
        // wait for the new source's first non-empty changeset so both reach subscribers as one batch; they
        // are published alone if Subscribe returns without one or the source terminates first.
        SingleAssignmentDisposable? handoffGeneration = null;
        var currentSubscription = new SerialDisposable();

        // Doubles as the generation token, published before any user code runs so that a reentrant
        // selection can cancel a subscription whose Subscribe has not yet returned.
        SingleAssignmentDisposable? currentGeneration = null;

        // A selection made while an earlier Subscribe or Dispose frame is still on the stack cannot start
        // yet: that frame owns resources it cannot release until it unwinds, so the activation already in
        // progress owns the handoff loop and picks the latest selection up when it gets there.
        IObservable<IChangeSet<TObject, TKey>>? pendingSource = null;
        var isActivating = false;

        // Selected but not yet completed, so the result cannot complete while a source is still pending.
        var hasOutstandingSource = false;
        var areSourcesComplete = false;
        var isStopped = false;

        IDisposable sourcesSubscription;
        try
        {
            sourcesSubscription = _sources.SubscribeSafe(Observer.Create<IObservable<IChangeSet<TObject, TKey>>>(SelectSource, error => Fail(error, null), SourcesCompleted));
        }
        catch (Exception error) when (!queue.HasDeliveryFailure)
        {
            try
            {
                // The error terminates the queue once delivered. It is not cleared or waited for: a delivery in
                // progress on another thread delivers it next, and its observer may need a lock the caller holds.
                Fail(error, null);
            }
            finally
            {
                currentSubscription.Dispose();
            }

            return Disposable.Empty;
        }
        catch
        {
            // Stop delivery without waiting for one in progress on another thread.
            queue.Terminate();
            currentSubscription.Dispose();

            throw;
        }

        return Disposable.Create(() =>
        {
            // Invalidate pending work before stopping delivery; this scope does not drain.
            using (queue.AcquireReadLock())
            {
                isStopped = true;
                pendingSource = null;
            }

            // Stop downstream delivery before tearing down the sources feeding it, without waiting for one
            // in progress on another thread: its observer may need a lock the caller holds.
            queue.Terminate();
            try
            {
                sourcesSubscription.Dispose();
            }
            finally
            {
                currentSubscription.Dispose();
            }
        });

        void SelectSource(IObservable<IChangeSet<TObject, TKey>> source)
        {
            SingleAssignmentDisposable generation;
            bool ownsHandoff;

            // Publish without draining: nothing may get ahead of releasing the previous source's resources.
            using (queue.AcquireReadLock())
            {
                if (isStopped || areSourcesComplete)
                {
                    return;
                }

                generation = new SingleAssignmentDisposable();
                currentGeneration = generation;
                hasOutstandingSource = true;

                // Latest selection always wins; only starting it can be deferred.
                pendingSource = source;
                ownsHandoff = !isActivating;
                isActivating = true;
            }

            // Disposing the outgoing subscription runs user teardown, so it happens outside the gate.
            currentSubscription.Disposable = generation;

            if (!ownsHandoff)
            {
                // An activation further down the stack starts this selection when it unwinds.
                return;
            }

            RunHandoffs();
        }

        // Starts selections one at a time, so a replacement never begins while a superseded frame unwinds.
        void RunHandoffs()
        {
            while (true)
            {
                IObservable<IChangeSet<TObject, TKey>>? source;
                SingleAssignmentDisposable? generation;

                using (queue.AcquireReadLock())
                {
                    source = pendingSource;
                    generation = currentGeneration;
                    pendingSource = null;

                    if (isStopped || source is null || generation is null)
                    {
                        isActivating = false;
                        return;
                    }
                }

                Activate(source, generation);
            }
        }

        void Activate(IObservable<IChangeSet<TObject, TKey>> source, SingleAssignmentDisposable generation)
        {
            // Nothing is published yet: the previous source's removals wait for the new source's snapshot.
            // A handoff left unpublished by a superseded activation is taken over, because current still
            // holds what subscribers last saw.
            using (queue.AcquireReadLock())
            {
                if (!IsCurrent(generation))
                {
                    return;
                }

                handoffGeneration = generation;
            }

            // Subscribe outside the gate and assign only to this generation's holder: if a newer source was
            // selected synchronously, the holder is already disposed, so this assignment releases the late
            // subscription and the handoff loop then starts the newer source.
            generation.Disposable = source.SubscribeSafe(Observer.Create<IChangeSet<TObject, TKey>>(
                changes =>
                {
                    using var scope = queue.AcquireLock();

                    if (!IsCurrent(generation))
                    {
                        return;
                    }

                    // An empty changeset carries no state, so the handoff waits for one that does.
                    if (changes.Count != 0 && ReferenceEquals(handoffGeneration, generation))
                    {
                        handoffGeneration = null;

                        if (current.Count != 0)
                        {
                            // The first non-empty changeset is the new source's snapshot: fold the removals into it.
                            var handoff = CreateHandoff(current, changes);
                            current.Clear();
                            current.Clone(changes);
                            scope.EnqueueNext(handoff);
                            return;
                        }
                    }

                    current.Clone(changes);

                    if (changes.Count != 0)
                    {
                        scope.EnqueueNext(changes);
                    }
                },
                error => Fail(error, generation),
                () =>
                {
                    using var scope = queue.AcquireLock();

                    if (!IsCurrent(generation))
                    {
                        return;
                    }

                    PublishPendingRemovals(scope);
                    hasOutstandingSource = false;

                    if (areSourcesComplete)
                    {
                        isStopped = true;
                        scope.EnqueueCompleted();
                    }
                }));

            // A source that has not delivered by the time Subscribe returns is delayed (or empty): its
            // changes follow the removals as separate batches when they arrive.
            using (var scope = queue.AcquireLock())
            {
                if (IsCurrent(generation))
                {
                    PublishPendingRemovals(scope);
                }
            }
        }

        // Publishes the current activation's unpublished removals on their own. Callers hold the gate.
        void PublishPendingRemovals(DeliveryQueue<IChangeSet<TObject, TKey>>.ScopedAccess scope)
        {
            if (handoffGeneration is null || !ReferenceEquals(handoffGeneration, currentGeneration))
            {
                return;
            }

            handoffGeneration = null;

            if (current.Count != 0)
            {
                scope.EnqueueNext(new ChangeSet<TObject, TKey>(
                    current.KeyValues.Select(static pair => new Change<TObject, TKey>(ChangeReason.Remove, pair.Key, pair.Value))));

                current.Clear();
            }
        }

        void SourcesCompleted()
        {
            using var scope = queue.AcquireLock();

            if (isStopped)
            {
                return;
            }

            areSourcesComplete = true;

            // No source outstanding also covers the case where none was ever selected.
            if (!hasOutstandingSource)
            {
                PublishPendingRemovals(scope);
                isStopped = true;
                scope.EnqueueCompleted();
            }
        }

        void Fail(Exception error, SingleAssignmentDisposable? generation)
        {
            using var scope = queue.AcquireLock();

            if (isStopped || (generation is not null && !ReferenceEquals(generation, currentGeneration)))
            {
                return;
            }

            // Removals already owed to subscribers precede the error, as they would have without folding.
            PublishPendingRemovals(scope);

            // Stop as soon as the terminal notification is queued, not only when it is delivered.
            isStopped = true;
            scope.EnqueueError(error);
        }

        // All callers hold the queue gate; notification handlers and handoffs use the same state.
        bool IsCurrent(SingleAssignmentDisposable generation) =>
            !isStopped && ReferenceEquals(generation, currentGeneration);
    });

    // One changeset taking subscribers from the previous source's items to the new source's snapshot.
    // Keys present in both become updates, so a bound row is replaced in place rather than removed and
    // re-added. The batch spans two sources' orderings, so it carries no indexes and drops moves.
    private static ChangeSet<TObject, TKey> CreateHandoff(Cache<TObject, TKey> previous, IChangeSet<TObject, TKey> snapshot)
    {
        HashSet<TKey>? replaced = null;
        foreach (var change in snapshot)
        {
            if (change.Reason == ChangeReason.Add && previous.Lookup(change.Key).HasValue)
            {
                (replaced ??= []).Add(change.Key);
            }
        }

        var result = new ChangeSet<TObject, TKey>(previous.Count + snapshot.Count);
        foreach (var pair in previous.KeyValues)
        {
            if (replaced?.Contains(pair.Key) != true)
            {
                result.Add(new Change<TObject, TKey>(ChangeReason.Remove, pair.Key, pair.Value));
            }
        }

        foreach (var change in snapshot)
        {
            switch (change.Reason)
            {
                case ChangeReason.Moved:
                    break;

                // Only the key's first addition replaces the previous value; a later one follows a removal.
                case ChangeReason.Add when replaced?.Remove(change.Key) == true:
                    result.Add(new Change<TObject, TKey>(ChangeReason.Update, change.Key, change.Current, previous.Lookup(change.Key)));
                    break;

                default:
                    result.Add(new Change<TObject, TKey>(change.Reason, change.Key, change.Current, change.Previous));
                    break;
            }
        }

        return result;
    }
}
