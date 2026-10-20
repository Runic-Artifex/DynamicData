// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Internal;
#else

namespace DynamicData.Internal;
#endif

/// <summary>
/// Provides SynchronizeSafe extension methods, drop-in replacements
/// for <c>Synchronize(lock)</c> that release the lock before downstream delivery.
/// </summary>
/// <remarks>
/// <para><strong>Disposal ordering matters.</strong> <c>CompositeDisposable</c> disposes in
/// declaration order. The queue and the source subscription have different roles:</para>
/// <list type="bullet">
///   <item>
///     <term>Subscription-first (SDQ overload)</term>
///     <description>The sub-queue is the <c>IObserver</c> that the source sends notifications to.
///     Disposing the subscription first allows any final terminal notification (OnCompleted/OnError
///     triggered by Rx's disposal cascade or a <c>Finally</c> operator) to flow through the
///     still-active sub-queue. The sub-queue is removed last as cleanup. Operators terminate the
///     shared queue itself first when they are disposed.</description>
///   </item>
///   <item>
///     <term>Terminate-first (gate overload)</term>
///     <description>The queue is terminated before the subscription is disposed, without waiting
///     for a delivery in progress on another thread, whose observer may need a lock the disposing
///     caller holds. That delivery finishes and nothing is delivered after it.</description>
///   </item>
///   <item>
///     <term>Terminate-first with deferred teardown (teardown overload)</term>
///     <description>Used by operators with teardown side effects (DisposeMany, OnBeingRemoved,
///     AsyncDisposeMany). The queue is terminated, the subscription disposed, and the teardown
///     (e.g., disposing the remaining items) runs once no delivery is in progress on another
///     thread: immediately, or on the delivering thread right after its delivery returns. Disposal
///     never waits for that delivery, and the teardown never runs while it may still use the items.
///     Terminal notifications are not needed because the subscriber is explicitly tearing down.</description>
///   </item>
/// </list>
/// </remarks>
internal static class SynchronizeSafeExtensions
{
    /// <summary>
    /// Synchronizes the source observable through a <see cref="SharedDeliveryQueue"/>.
    /// Use when multiple sources of different types share a gate.
    /// </summary>
    /// <typeparam name="T">The type of the T value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="queue">The queue value.</param>
    /// <returns>The result of the operation.</returns>
    public static IObservable<T> SynchronizeSafe<T>(this IObservable<T> source, SharedDeliveryQueue queue)
        =>
        Observable.Create<T>(observer =>
        {
            var subQueue = queue.CreateQueue(observer);

            // Subscription first: terminal notifications flow through the still-active sub-queue
            return new CompositeDisposable(source.SubscribeSafe(subQueue), subQueue);
        });

    /// <summary>
    /// Synchronizes the source observable through an implicitly created <c>DeliveryQueue&lt;T&gt;</c>.
    /// Drop-in replacement for <c>Synchronize(locker)</c>.
    /// </summary>
    /// <typeparam name="T">The type of the T value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="gate">The gate value.</param>
    /// <returns>The result of the operation.</returns>
    public static IObservable<T> SynchronizeSafe<T>(this IObservable<T> source, Lock gate)
        =>
        Observable.Create<T>(observer =>
        {
            var queue = new DeliveryQueue<T>(gate, observer);

            // Terminate first, without waiting for a delivery in progress on another thread.
            return new CompositeDisposable(Disposable.Create(queue.Terminate), source.SubscribeSafe(queue));
        });

    /// <summary>
    /// Synchronizes the source observable through an implicitly created <c>DeliveryQueue&lt;T&gt;</c> whose
    /// deliveries <paramref name="teardown"/> keeps track of. Disposing the subscription stops further delivery
    /// without waiting for one in progress on another thread; the operator then runs its teardown through
    /// <see cref="QueuedTeardown.Run"/>, which defers it to the delivering thread until that delivery returns.
    /// </summary>
    /// <typeparam name="T">The type of the T value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <param name="teardown">Tracks the queue for the operator's teardown. One per subscription.</param>
    /// <returns>The result of the operation.</returns>
    public static IObservable<T> SynchronizeSafe<T>(this IObservable<T> source, QueuedTeardown teardown)
        =>
        Observable.Create<T>(observer =>
        {
            var queue = new DeliveryQueue<T>(observer);
            teardown.Attach(queue.RunWhenNotDelivering);

            // Terminate first, without waiting for a delivery in progress on another thread.
            return new CompositeDisposable(Disposable.Create(queue.Terminate), source.SubscribeSafe(queue));
        });

    // Merges every input into a single observable without taking any synchronization gate.
    // Functionally equivalent to Observable.Merge: completes only after every source completes,
    // the first error terminates, subscription occurs in argument order.
    //
    // The caller MUST ensure that delivery from every source is already serialized. In this
    // library the precondition is satisfied by routing every source through the same
    // SharedDeliveryQueue via SynchronizeSafe(queue). The shared queue's drain loop guarantees
    // that at most one notification is in flight to the downstream observer at a time, so the
    // additional gate that Observable.Merge would install is redundant.
    //
    // The gate omission matters in cross-cache pipelines: Observable.Merge holds its private
    // _gate for the entire duration of downstream delivery, and when downstream delivery walks
    // into another cache's writer lock, two such gates on two operators form an ABBA cycle that
    // the queue-drain design is meant to prevent.
    //
    // Without the external serialization precondition, concurrent OnNext calls into the shared
    // observer will race. Do not use as a general-purpose Observable.Merge replacement.
    public static IObservable<T> UnsynchronizedMerge<T>(this IObservable<T> first, params IObservable<T>[] others) =>
        Observable.Create<T>(observer =>
        {
            var remainingSources = others.Length + 1;
            var subscriptions = new CompositeDisposable();
            var terminated = 0;

            // Runic (DD-044, fork issue #37): what the observer threw, so that it is thrown again rather than
            // dropped when a System.Reactive subscribe safeguard delivers it back as OnError.
            DeliveryFault? fault = null;

            try
            {
                subscriptions.Add(first.SubscribeSafe(CreateInner()));
                foreach (var source in others)
                {
                    subscriptions.Add(source.SubscribeSafe(CreateInner()));
                }
            }
            catch
            {
                // Runic (DD-044, fork issue #36): an exception thrown while an input is subscribed, such as a
                // subscriber's own exception at a value it delivers, must not leave the earlier inputs subscribed.
                subscriptions.Dispose();
                throw;
            }

            return subscriptions;

            // Each source needs its own inner observer instance because Rx's ObserverBase sets
            // a one-shot stopped flag on the first OnCompleted or OnError. A single shared
            // observer would silently drop terminal notifications from every source after the
            // first. The OnNext/OnError/OnCompleted actions close over the shared remainingSources
            // and terminated counters so cross-source coordination still works.
            IObserver<T> CreateInner() => Observer.Create<T>(OnNextSafe, OnErrorSafe, OnCompletedSafe);

            void OnNextSafe(T value)
            {
                if (Volatile.Read(ref terminated) != 0)
                {
                    return;
                }

                try
                {
                    observer.OnNext(value);
                }
                catch (Exception error)
                {
                    DeliveryFault.Record(ref fault, error);
                    throw;
                }
            }

            void OnErrorSafe(Exception error)
            {
                DeliveryFault.ThrowIfReturned(Volatile.Read(ref fault), error);
                if (Interlocked.Exchange(ref terminated, 1) == 0)
                {
                    observer.OnError(error);
                }
            }

            void OnCompletedSafe()
            {
                if (Interlocked.Decrement(ref remainingSources) == 0 && Interlocked.Exchange(ref terminated, 1) == 0)
                {
                    observer.OnCompleted();
                }
            }
        });

    // Two-input CombineLatest variant that does NOT install a gate. Functionally equivalent
    // to Rx CombineLatest in both flavors: holds the most-recent value from each source, emits a
    // resultSelector output whenever either source fires (provided the other has also fired
    // at least once), selector failures are reported as errors, and completion follows Rx:
    // both sources completed, or a value arrives after the other completed without a value.
    //
    // Same precondition as UnsynchronizedMerge: delivery from BOTH sources must already be
    // serialized through the same external gate before reaching this operator. In this library
    // that is satisfied by routing both inputs through the same SharedDeliveryQueue via
    // SynchronizeSafe(queue). Under that precondition no two OnNext calls overlap, so the
    // latest-value state needs no internal locking, and the gate that
    // Observable.CombineLatest installs becomes redundant.
    //
    // The Rx gate matters here for the same reason as Merge: Observable.CombineLatest holds
    // its private _gate for the entire downstream delivery, and any operator-level lock held
    // across a cross-cache write reconstructs the ABBA cycle the queue-drain design is meant
    // to prevent.
    //
    // Without the external serialization precondition, concurrent OnNext calls would race the
    // latest-value state and could produce torn reads. Do not use as a general-purpose
    // Observable.CombineLatest replacement.
    public static IObservable<TResult> UnsynchronizedCombineLatest<TFirst, TSecond, TResult>(
        this IObservable<TFirst> first,
        IObservable<TSecond> second,
        Func<TFirst, TSecond, TResult> resultSelector)
        where TFirst : notnull
        where TSecond : notnull =>
        Observable.Create<TResult>(observer =>
        {
            TFirst firstLatest = default!;
            TSecond secondLatest = default!;
            var hasFirst = false;
            var hasSecond = false;
            var firstCompleted = false;
            var secondCompleted = false;
            var terminated = 0;

            // Runic (DD-044, fork issue #37): see UnsynchronizedMerge.
            DeliveryFault? fault = null;

            var subscriptions = new CompositeDisposable();
            try
            {
                subscriptions.Add(first.SubscribeSafe(Observer.Create<TFirst>(OnFirstNext, OnErrorSafe, OnFirstCompleted)));
                subscriptions.Add(second.SubscribeSafe(Observer.Create<TSecond>(OnSecondNext, OnErrorSafe, OnSecondCompleted)));
            }
            catch
            {
                // Runic (DD-044, fork issue #36): see UnsynchronizedMerge.
                subscriptions.Dispose();
                throw;
            }

            return subscriptions;

            void OnFirstNext(TFirst value)
            {
                if (Volatile.Read(ref terminated) != 0)
                {
                    return;
                }

                firstLatest = value;
                hasFirst = true;
                EmitLatestOrComplete(secondCompleted);
            }

            void OnSecondNext(TSecond value)
            {
                if (Volatile.Read(ref terminated) != 0)
                {
                    return;
                }

                secondLatest = value;
                hasSecond = true;
                EmitLatestOrComplete(firstCompleted);
            }

            void EmitLatestOrComplete(bool otherCompleted)
            {
                if (hasFirst && hasSecond)
                {
                    TResult result;
                    try
                    {
                        result = resultSelector(firstLatest, secondLatest);
                    }
                    catch (Exception error)
                    {
                        OnErrorSafe(error);
                        return;
                    }

                    try
                    {
                        observer.OnNext(result);
                    }
                    catch (Exception error)
                    {
                        DeliveryFault.Record(ref fault, error);
                        throw;
                    }
                }
                else if (otherCompleted)
                {
                    Complete();
                }
            }

            void OnErrorSafe(Exception error)
            {
                DeliveryFault.ThrowIfReturned(Volatile.Read(ref fault), error);
                if (Interlocked.Exchange(ref terminated, 1) == 0)
                {
                    observer.OnError(error);
                }
            }

            void OnFirstCompleted()
            {
                firstCompleted = true;
                if (secondCompleted)
                {
                    Complete();
                }
            }

            void OnSecondCompleted()
            {
                secondCompleted = true;
                if (firstCompleted)
                {
                    Complete();
                }
            }

            void Complete()
            {
                if (Interlocked.Exchange(ref terminated, 1) == 0)
                {
                    observer.OnCompleted();
                }
            }
        });
}
