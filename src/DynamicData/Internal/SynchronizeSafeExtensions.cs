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
///     <term>Subscription-first (gate and SDQ overloads)</term>
///     <description>The queue is the <c>IObserver</c> that the source sends notifications to.
///     Disposing the subscription first allows any final terminal notification (OnCompleted/OnError
///     triggered by Rx's disposal cascade or a <c>Finally</c> operator) to flow through the
///     still-active queue. The queue is disposed last as cleanup.</description>
///   </item>
///   <item>
///     <term>Queue-first (parameterless overload)</term>
///     <description>Used by operators with teardown side effects (DisposeMany, OnBeingRemoved).
///     The queue is terminated first via <c>DeliveryQueue&lt;T&gt;.Dispose</c>, which ensures
///     all in-flight deliveries complete before the subscription is disposed and teardown logic
///     (e.g., disposing removed items) runs. Terminal notifications are not needed because
///     the subscriber is explicitly tearing down.</description>
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

            // Subscription first: terminal notifications flow through the still-active queue
            return new CompositeDisposable(source.SubscribeSafe(queue), queue);
        });

    /// <summary>
    /// Synchronizes the source observable through an implicitly created <c>DeliveryQueue&lt;T&gt;</c>
    /// with automatic delivery completion on dispose. The queue is terminated and drained
    /// before the source subscription is disposed, ensuring all in-flight notifications
    /// are delivered before teardown.
    /// </summary>
    /// <typeparam name="T">The type of the T value.</typeparam>
    /// <param name="source">The source value.</param>
    /// <returns>The result of the operation.</returns>
    public static IObservable<T> SynchronizeSafe<T>(this IObservable<T> source)
        =>
        Observable.Create<T>(observer =>
        {
            var queue = new DeliveryQueue<T>(observer);

            // Queue first: ensures in-flight deliveries complete before teardown side effects run
            return new CompositeDisposable(queue, source.SubscribeSafe(queue));
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

            subscriptions.Add(first.SubscribeSafe(CreateInner()));
            foreach (var source in others)
            {
                subscriptions.Add(source.SubscribeSafe(CreateInner()));
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
                if (Volatile.Read(ref terminated) == 0)
                {
                    observer.OnNext(value);
                }
            }

            void OnErrorSafe(Exception error)
            {
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
    // to Observable.CombineLatest: holds the most-recent value from each source, emits a
    // resultSelector output whenever either source fires (provided the other has also fired
    // at least once), the first error terminates, completes when both sources complete.
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
            var firstLatest = ReactiveUI.Primitives.Optional<TFirst>.None;
            var secondLatest = ReactiveUI.Primitives.Optional<TSecond>.None;
            var remainingSources = 2;
            var terminated = 0;

            var subscriptions = new CompositeDisposable();
            subscriptions.Add(first.SubscribeSafe(Observer.Create<TFirst>(OnFirstNext, OnErrorSafe, OnCompletedSafe)));
            subscriptions.Add(second.SubscribeSafe(Observer.Create<TSecond>(OnSecondNext, OnErrorSafe, OnCompletedSafe)));
            return subscriptions;

            void OnFirstNext(TFirst value)
            {
                if (Volatile.Read(ref terminated) != 0)
                {
                    return;
                }

                firstLatest = value;
                if (secondLatest.HasValue)
                {
                    observer.OnNext(resultSelector(value, secondLatest.Value));
                }
            }

            void OnSecondNext(TSecond value)
            {
                if (Volatile.Read(ref terminated) != 0)
                {
                    return;
                }

                secondLatest = value;
                if (firstLatest.HasValue)
                {
                    observer.OnNext(resultSelector(firstLatest.Value, value));
                }
            }

            void OnErrorSafe(Exception error)
            {
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
}
