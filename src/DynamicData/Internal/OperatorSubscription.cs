// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM

namespace DynamicData.Reactive.Internal;
#else
namespace DynamicData.Internal;
#endif

/// <summary>
/// Runic (DD-044, fork issue #25): callback subscriptions for operators, shared by both flavours,
/// in place of the static <c>PrimitivesLinqExtensions.SubscribeSafe(source, onNext, ...)</c>.
/// </summary>
/// <remarks>
/// <para>
/// ReactiveUI.Primitives' static <c>SubscribeSafe</c> catches every exception thrown by <c>onNext</c> and
/// passes it to <c>onError</c>. That suits an operator's own callbacks (predicates, selectors,
/// comparers, item disposal), which the library delivers as <c>OnError</c>, but it also caught the
/// exception of a subscriber further downstream: the Primitives flavour delivered it to that subscriber
/// as <c>OnError</c>, and the System.Reactive flavour (ReactiveUI.Primitives.Reactive) lost it.
/// </para>
/// <para>
/// Here an exception thrown by <c>onNext</c> is still delivered to <c>onError</c> (and ends the
/// subscription), unless it was thrown by an observer wrapped with <see cref="SubscriberFault.Guard"/>:
/// such a subscriber exception propagates to the producer, as in System.Reactive, through every
/// operator subscription on the way. Only the subscribe call itself is otherwise guarded.
/// </para>
/// </remarks>
internal static class OperatorSubscription
{
    private static readonly Action NoCompletion = static () => { };

    /// <summary>
    /// Creates an operator observable whose observer is guarded with <see cref="SubscriberFault.Guard"/>:
    /// an exception thrown by the subscriber propagates to the producer and detaches the subscription,
    /// as System.Reactive's <c>Observable.Create</c> does.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="subscribe">Subscribes the guarded observer and returns the subscription.</param>
    /// <returns>The observable.</returns>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        ArgumentNullException.ThrowIfNull(subscribe);

        return Observable.Create<T>(observer =>
        {
            var guard = SubscriberFault.Guard(observer);
            guard.DetachOnFault(subscribe(guard));
            return guard;
        });
    }

    /// <summary>
    /// Subscribes the callbacks to the source. An exception thrown by <paramref name="onNext"/> is
    /// delivered to <paramref name="onError"/> unless it is a subscriber exception.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The callback for each value.</param>
    /// <param name="onError">The callback for an error.</param>
    /// <returns>A disposable that ends the subscription.</returns>
    public static IDisposable SubscribeSafe<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError) =>
        SubscribeSafe(source, onNext, onError, NoCompletion);

    /// <summary>
    /// Subscribes the callbacks to the source. An exception thrown by <paramref name="onNext"/> is
    /// delivered to <paramref name="onError"/> unless it is a subscriber exception.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The callback for each value.</param>
    /// <param name="onError">The callback for an error.</param>
    /// <param name="onCompleted">The callback for completion.</param>
    /// <returns>A disposable that ends the subscription.</returns>
    public static IDisposable SubscribeSafe<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(onNext);
        ArgumentNullException.ThrowIfNull(onError);
        ArgumentNullException.ThrowIfNull(onCompleted);

        var sink = new Sink<T>(onNext, onError, onCompleted);
        IDisposable upstream;
        try
        {
            upstream = source.Subscribe(sink);
        }
        catch (Exception error) when (!SubscriberFault.IsSubscriberFault(error))
        {
            sink.OnError(error);
            return sink;
        }

        sink.SetUpstream(upstream);
        return sink;
    }

    private sealed class Sink<T>(Action<T> onNext, Action<Exception> onError, Action onCompleted) : IObserver<T>, IDisposable
    {
        private static readonly IDisposable Disposed = Disposable.Empty;

        private IDisposable? _upstream;
        private volatile bool _stopped;

        public void SetUpstream(IDisposable upstream)
        {
            if (Interlocked.CompareExchange(ref _upstream, upstream, null) is not null)
            {
                // Terminated or disposed during Subscribe.
                upstream.Dispose();
            }
        }

        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            try
            {
                onNext(value);
            }
            catch (Exception error) when (!SubscriberFault.IsSubscriberFault(error))
            {
                // The operator's own failure: deliver it as OnError and stop, as Primitives' SubscribeSafe did.
                OnError(error);
            }
        }

        public void OnError(Exception error)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            try
            {
                onError(error);
            }
            finally
            {
                DisposeUpstream();
            }
        }

        public void OnCompleted()
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            try
            {
                onCompleted();
            }
            finally
            {
                DisposeUpstream();
            }
        }

        public void Dispose()
        {
            _stopped = true;
            DisposeUpstream();
        }

        private void DisposeUpstream() => Interlocked.Exchange(ref _upstream, Disposed)?.Dispose();
    }
}

/// <summary>
/// Runic (DD-044, fork issue #25): identifies exceptions thrown by an operator's downstream observer, so
/// <see cref="OperatorSubscription"/> lets them propagate to the producer instead of delivering them as
/// <c>OnError</c>.
/// </summary>
internal static class SubscriberFault
{
    private static readonly ConditionalWeakTable<Exception, object> Faults = [];
    private static readonly object Marker = new();

    /// <summary>
    /// Wraps an operator's downstream observer. An exception thrown by its <c>OnNext</c>,
    /// <c>OnError</c> or <c>OnCompleted</c> is marked as a subscriber exception, detaches the
    /// subscription passed to <see cref="GuardedObserver{T}.DetachOnFault"/> and is rethrown.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="downstream">The downstream observer.</param>
    /// <returns>The guarded observer.</returns>
    public static GuardedObserver<T> Guard<T>(IObserver<T> downstream) => new(downstream);

    /// <summary>
    /// Gets a value indicating whether the exception was thrown by a guarded downstream observer.
    /// </summary>
    /// <param name="error">The exception.</param>
    /// <returns><see langword="true"/> for a subscriber exception.</returns>
    public static bool IsSubscriberFault(Exception error) => Faults.TryGetValue(error, out _);

    private static void Mark(Exception error) => Faults.AddOrUpdate(error, Marker);

    /// <summary>
    /// A downstream observer that marks the exceptions it throws as subscriber exceptions.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="downstream">The downstream observer.</param>
    internal sealed class GuardedObserver<T>(IObserver<T> downstream) : IObserver<T>, IDisposable
    {
        private static readonly IDisposable Detached = Disposable.Empty;

        private IDisposable? _subscription;
        private DeliveryFault? _fault;
        private volatile bool _faulted;

        /// <summary>
        /// Sets the subscription to dispose when the downstream observer throws, or when this observer
        /// is disposed. It is disposed immediately if that already happened.
        /// </summary>
        /// <param name="subscription">The operator's subscription.</param>
        public void DetachOnFault(IDisposable subscription)
        {
            if (Interlocked.CompareExchange(ref _subscription, subscription, null) is not null)
            {
                subscription.Dispose();
            }
        }

        public void OnNext(T value)
        {
            if (_faulted)
            {
                return;
            }

            try
            {
                downstream.OnNext(value);
            }
            catch (Exception error)
            {
                Fault(error);
                throw;
            }
        }

        public void OnError(Exception error)
        {
            if (_faulted)
            {
                // Runic (fork issue #37): the subscriber's own exception, delivered back by a System.Reactive
                // subscribe safeguard, goes to the producer.
                DeliveryFault.ThrowIfReturned(Volatile.Read(ref _fault), error);
                return;
            }

            try
            {
                downstream.OnError(error);
            }
            catch (Exception thrown)
            {
                Fault(thrown);
                throw;
            }
        }

        public void OnCompleted()
        {
            if (_faulted)
            {
                return;
            }

            try
            {
                downstream.OnCompleted();
            }
            catch (Exception error)
            {
                Fault(error);
                throw;
            }
        }

        public void Dispose() => Interlocked.Exchange(ref _subscription, Detached)?.Dispose();

        private void Fault(Exception error)
        {
            // Like System.Reactive's subscription safeguard: an observer that throws is detached before
            // the exception propagates to the producer, and receives nothing further.
            Mark(error);
            DeliveryFault.Record(ref _fault, error);
            _faulted = true;
            Dispose();
        }
    }
}

/// <summary>
/// Runic (DD-044, fork issue #34): carries a subscriber exception out of an operator step that delivers
/// every exception as <c>OnError</c>, such as a <c>Transform</c> factory, which runs inside <c>Scan</c> in
/// both flavours.
/// </summary>
/// <remarks>
/// <c>MergeMany</c> subscribes each child inside <c>SubscribeMany</c>'s transform factory, so a child that
/// emits while it is subscribed (<c>Observable.Return</c>, <c>WhenValueChanged</c> with its initial value)
/// delivers to the subscriber from inside that factory. <see cref="TryCapture"/> takes a subscriber
/// exception thrown there, and the observable returned by <see cref="RethrowAfter{T}"/> throws it to the
/// producer once the source notification being processed returns, as for a value a child emits later.
/// Source notifications must be serialized and the capture made while one is being processed.
/// </remarks>
internal sealed class SubscriberFaultRelay
{
    private Exception? _fault;

    /// <summary>
    /// Captures <paramref name="error"/> if it is a subscriber exception, for <see cref="RethrowAfter{T}"/>
    /// to rethrow.
    /// </summary>
    /// <param name="error">The exception.</param>
    /// <returns><see langword="true"/> if it was captured; otherwise the caller handles it.</returns>
    public bool TryCapture(Exception error)
    {
        if (!SubscriberFault.IsSubscriberFault(error))
        {
            return false;
        }

        Interlocked.CompareExchange(ref _fault, error, null);
        return true;
    }

    /// <summary>
    /// Forwards the source and, after each notification, rethrows a subscriber exception captured while
    /// it was processed.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The serialized source.</param>
    /// <returns>The source, rethrowing captured subscriber exceptions to its producer.</returns>
    public IObservable<T> RethrowAfter<T>(IObservable<T> source) =>
        Observable.Create<T>(observer => source.Subscribe(new RelayObserver<T>(observer, this)));

    private void RethrowCaptured()
    {
        if (Interlocked.Exchange(ref _fault, null) is { } fault)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(fault);
        }
    }

    private sealed class RelayObserver<T>(IObserver<T> downstream, SubscriberFaultRelay relay) : IObserver<T>
    {
        public void OnNext(T value)
        {
            downstream.OnNext(value);
            relay.RethrowCaptured();
        }

        public void OnError(Exception error)
        {
            downstream.OnError(error);
            relay.RethrowCaptured();
        }

        public void OnCompleted()
        {
            downstream.OnCompleted();
            relay.RethrowCaptured();
        }
    }
}

/// <summary>
/// Runic (DD-044, fork issue #37): the exception a delivery point's downstream observer threw, so that the point
/// throws it again, instead of delivering it, when the exception comes back to it as <c>OnError</c>.
/// </summary>
/// <remarks>
/// System.Reactive's <c>Observable.Create</c> and <c>SubscribeSafe</c> deliver an exception thrown while they
/// subscribe as <c>OnError</c> to their observer, unless that observer threw it itself. A subscriber exception
/// thrown during the initial snapshot can leave such a subscribe call without passing through its observer, for
/// example when a shared delivery queue delivers another input's value, when a <c>MergeMany</c> child is
/// subscribed or when <c>Concat</c> subscribes an inner source. The <c>OnError</c> then reached the delivery queue
/// or guard that the exception had passed through, which forwarded it to the subscriber that had already thrown,
/// or dropped it, and <c>Subscribe</c> returned normally. Only an exception returning on the thread that threw it
/// is thrown again; it propagates to the producer, which for the initial snapshot is the caller of
/// <c>Subscribe</c>.
/// </remarks>
internal sealed class DeliveryFault
{
    private readonly Exception _error;
    private readonly int _threadId;

    private DeliveryFault(Exception error)
    {
        _error = error;
        _threadId = Environment.CurrentManagedThreadId;
    }

    /// <summary>
    /// Records the exception a downstream observer threw while a notification was delivered to it.
    /// </summary>
    /// <param name="slot">The delivery point's record.</param>
    /// <param name="error">The exception.</param>
    public static void Record(ref DeliveryFault? slot, Exception error) => Volatile.Write(ref slot, new DeliveryFault(error));

    /// <summary>
    /// Throws <paramref name="error"/> if it is the recorded exception, returning on the thread that threw it.
    /// </summary>
    /// <param name="fault">The delivery point's record.</param>
    /// <param name="error">The error about to be delivered.</param>
    public static void ThrowIfReturned(DeliveryFault? fault, Exception error)
    {
        if (fault is not null && ReferenceEquals(fault._error, error) && fault._threadId == Environment.CurrentManagedThreadId)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(error);
        }
    }
}
