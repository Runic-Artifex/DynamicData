// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.Cache.Internal;
#else

namespace DynamicData.Cache.Internal;
#endif

/// <summary>
/// Provides members for the MergeMany class.
/// </summary>
/// <typeparam name="TObject">The type of the TObject value.</typeparam>
/// <typeparam name="TKey">The type of the TKey value.</typeparam>
/// <typeparam name="TDestination">The type of the TDestination value.</typeparam>
internal sealed class MergeMany<TObject, TKey, TDestination>
    where TObject : notnull
    where TKey : notnull
{
    /// <summary>
    /// The _observableSelector field.
    /// </summary>
    private readonly Func<TObject, TKey, IObservable<TDestination>> _observableSelector;

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly IObservable<IChangeSet<TObject, TKey>> _source;

    /// <summary>
    /// Initializes a new instance of the <see cref="MergeMany{TObject, TKey, TDestination}"/> class.
    /// </summary>
    /// <param name="source">The source value.</param>
    /// <param name="observableSelector">The observableSelector value.</param>
    public MergeMany(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, TKey, IObservable<TDestination>> observableSelector)
    {
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _observableSelector = observableSelector ?? throw new ArgumentNullException(nameof(observableSelector));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MergeMany{TObject, TKey, TDestination}"/> class.
    /// </summary>
    /// <param name="source">The source value.</param>
    /// <param name="observableSelector">The observableSelector value.</param>
    public MergeMany(IObservable<IChangeSet<TObject, TKey>> source, Func<TObject, IObservable<TDestination>> observableSelector)
    {
        ArgumentNullException.ThrowIfNull(observableSelector);

        _source = source ?? throw new ArgumentNullException(nameof(source));
        _observableSelector = (t, _) => observableSelector(t);
    }

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    /// <remarks>
    /// Runic (DD-044, fork issue #34): the observer is guarded, so its own exception is thrown to the producer
    /// and detaches the subscription, also when a child delivers to it while it is being subscribed.
    /// </remarks>
    public IObservable<TDestination> Run() => OperatorSubscription.Create<TDestination>(
            observer =>
            {
                var counter = new StrongBox<int>(1);
                var queue = new DeliveryQueue<TDestination>(observer);
                var faults = new SubscriberFaultRelay();

                // Queue first: terminate before subscription disposal to prevent
                // Finally callbacks from delivering spurious OnCompleted during teardown.
                return new CompositeDisposable(Disposable.Create(queue.Terminate), faults.RethrowAfter(_source)
                    .Do(static _ => { }, static _ => { }, () => CheckCompleted(counter, queue))
                    .Concat(Observable.Never<IChangeSet<TObject, TKey>>())
                    .SubscribeMany((t, key) =>
                    {
                        Interlocked.Increment(ref counter.Value);
                        try
                        {
                            return _observableSelector(t, key)
                                .Finally(() => CheckCompleted(counter, queue))
                                .Subscribe(queue.OnNext, queue.OnError);
                        }
                        catch (Exception error) when (faults.TryCapture(error))
                        {
                            // The subscriber threw at a value the child emitted while being subscribed. Thrown from
                            // here, SubscribeMany's transform would deliver it as OnError; the relay throws it to
                            // the producer.
                            return Disposable.Empty;
                        }
                    })
                    .Subscribe(static _ => { }, queue.OnError));
            });

    /// <summary>
    /// Executes the CheckCompleted operation.
    /// </summary>
    /// <param name="counter">The counter value.</param>
    /// <param name="queue">The queue value.</param>
    private static void CheckCompleted(StrongBox<int> counter, DeliveryQueue<TDestination> queue)
    {
        if (Interlocked.Decrement(ref counter.Value) == 0 && !queue.IsTerminated)
        {
            queue.OnCompleted();
        }
    }
}
