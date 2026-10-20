// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the Switch class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="sources">The sources value.</param>
internal sealed class Switch<T>(IObservable<IObservable<IChangeSet<T>>> sources)
    where T : notnull
{
    /// <summary>
    /// The _sources field.
    /// </summary>
    private readonly IObservable<IObservable<IChangeSet<T>>> _sources = sources ?? throw new ArgumentNullException(nameof(sources));

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IChangeSet<T>> Run() => Observable.Create<IChangeSet<T>>(
            observer =>
            {
                // Switching is done by hand rather than with Observable.Switch, which holds its gate for
                // the whole of downstream delivery. The queue enqueues and returns instead, so a producer
                // is never held up by whatever a subscriber does with the notification, and a pipeline
                // crossing into another collection cannot deadlock against it.
                var queue = new DeliveryQueue<IChangeSet<T>>(observer);

                // What has been published downstream, so that switching away can take it back out.
                var current = new List<T>();
                var subscription = new SerialDisposable();

                // The source whose clearing of the previous items is still unpublished. The clear waits for
                // the new source's first non-empty changeset so both reach subscribers as one batch; it is
                // published alone if Subscribe returns without one or the source terminates first.
                var handoff = 0;

                // Identifies the current source. A superseded one may still be mid-delivery, and anything
                // it produces after this point belongs to a source that has already been switched away from.
                var active = 0;
                var isSourceRunning = false;
                var areSourcesComplete = false;

                var outer = _sources.SubscribeSafe(Observer.Create<IObservable<IChangeSet<T>>>(
                    source =>
                    {
                        int id;

                        // Nothing is published yet. A clear left unpublished by a superseded source is taken
                        // over, because current still holds what subscribers last saw.
                        using (queue.AcquireReadLock())
                        {
                            id = ++active;
                            isSourceRunning = true;
                            handoff = id;
                        }

                        // Subscribed outside the lock. The source may deliver synchronously, and that
                        // delivery takes the lock for itself.
                        if (id != Volatile.Read(ref active))
                        {
                            return;
                        }

                        var innerSubscription = new SingleAssignmentDisposable();
                        subscription.Disposable = innerSubscription;
                        innerSubscription.Disposable = source.SubscribeSafe(Observer.Create<IChangeSet<T>>(
                            changes =>
                            {
                                using var scope = queue.AcquireLock();

                                if (id != active)
                                {
                                    return;
                                }

                                // An empty changeset carries no state, so the handoff waits for one that does.
                                if (changes.Count != 0 && handoff == id)
                                {
                                    handoff = 0;

                                    if (current.Count != 0)
                                    {
                                        // The first non-empty changeset is the new source's snapshot: it follows
                                        // the clear in the same batch, and its indexes start from the empty list.
                                        var batch = new ChangeSet<T>(changes.Count + 1) { new Change<T>(ListChangeReason.Clear, current.ToArray()) };
                                        batch.AddRange(changes);
                                        current.Clear();
                                        current.Clone(changes);
                                        scope.EnqueueNext(batch);
                                        return;
                                    }
                                }

                                current.Clone(changes);

                                if (changes.Count != 0)
                                {
                                    scope.EnqueueNext(changes);
                                }
                            },
                            error =>
                            {
                                using var scope = queue.AcquireLock();

                                if (id != active)
                                {
                                    return;
                                }

                                // A clear already owed to subscribers precedes the error.
                                PublishPendingClear(scope);
                                scope.EnqueueError(error);
                            },
                            () =>
                            {
                                using var scope = queue.AcquireLock();

                                if (id != active)
                                {
                                    return;
                                }

                                PublishPendingClear(scope);
                                isSourceRunning = false;

                                if (areSourcesComplete)
                                {
                                    scope.EnqueueCompleted();
                                }
                            }));

                        // A source that has not delivered by the time Subscribe returns is delayed (or empty):
                        // its changes follow the clear as separate batches when they arrive.
                        using (var scope = queue.AcquireLock())
                        {
                            if (id == active)
                            {
                                PublishPendingClear(scope);
                            }
                        }
                    },
                    error =>
                    {
                        using var scope = queue.AcquireLock();

                        PublishPendingClear(scope);
                        scope.EnqueueError(error);
                    },
                    () =>
                    {
                        using var scope = queue.AcquireLock();

                        areSourcesComplete = true;

                        // The current source may still be running, and the result ends only once both have.
                        if (!isSourceRunning)
                        {
                            scope.EnqueueCompleted();
                        }
                    }));

                // Disposal order matters and CompositeDisposable does not specify one. The queue goes first
                // so that any delivery in flight is finished before the subscriptions feeding it are torn down.
                return Disposable.Create(() =>
                {
                    queue.Dispose();
                    outer.Dispose();
                    subscription.Dispose();
                });

                // Publishes the current source's unpublished clear on its own. Callers hold the gate.
                void PublishPendingClear(DeliveryQueue<IChangeSet<T>>.ScopedAccess scope)
                {
                    if (handoff == 0 || handoff != active)
                    {
                        return;
                    }

                    handoff = 0;

                    if (current.Count != 0)
                    {
                        scope.EnqueueNext(new ChangeSet<T> { new Change<T>(ListChangeReason.Clear, current.ToArray()) });
                        current.Clear();
                    }
                }
            });
}
