// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

// Runic (DD-044): deliberately in the root DynamicData namespace so that every source.Do(...)
// extension call in DynamicData.* code binds here instead of ReactiveUI.Primitives' Do (see
// ScanExtensions). The System.Reactive flavour keeps Observable.Do.
namespace DynamicData;

/// <summary>
/// Primitives-flavour replacement for <c>Do</c> that follows the System.Reactive contract.
/// </summary>
/// <remarks>
/// ReactiveUI.Primitives 8.4 and 9.0 <c>Do</c> does not catch callback exceptions: they are thrown
/// back to the producer instead of being delivered as <c>OnError</c>, so failures in callbacks such as
/// <c>OnItemAdded</c> and <c>ForEachChange</c> were thrown to the editor or lost. This implementation
/// delivers callback failures as <c>OnError</c> and lets downstream exceptions propagate.
/// </remarks>
internal static class DoExtensions
{
    private static readonly Action<Exception> NoError = static _ => { };
    private static readonly Action NoCompletion = static () => { };

    /// <summary>Invokes an action for each value. An exception thrown by the action is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <returns>The source sequence with the side effect.</returns>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext) =>
        Create(source, onNext, NoError, NoCompletion);

    /// <summary>Invokes actions for each value and for an error. An exception thrown by an action is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <param name="onError">The action invoked for an error.</param>
    /// <returns>The source sequence with the side effects.</returns>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError)
    {
        ArgumentNullException.ThrowIfNull(onError);
        return Create(source, onNext, onError, NoCompletion);
    }

    /// <summary>Invokes actions for each value and for completion. An exception thrown by an action is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <param name="onCompleted">The action invoked for completion.</param>
    /// <returns>The source sequence with the side effects.</returns>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext, Action onCompleted)
    {
        ArgumentNullException.ThrowIfNull(onCompleted);
        return Create(source, onNext, NoError, onCompleted);
    }

    /// <summary>Invokes actions for each notification. An exception thrown by an action is delivered as <c>OnError</c>.</summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="onNext">The action invoked for each value.</param>
    /// <param name="onError">The action invoked for an error.</param>
    /// <param name="onCompleted">The action invoked for completion.</param>
    /// <returns>The source sequence with the side effects.</returns>
    public static IObservable<T> Do<T>(this IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentNullException.ThrowIfNull(onError);
        ArgumentNullException.ThrowIfNull(onCompleted);
        return Create(source, onNext, onError, onCompleted);
    }

    private static DoObservable<T> Create<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(onNext);

        return new DoObservable<T>(source, onNext, onError, onCompleted);
    }

    private sealed class DoObservable<T>(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted) : IObservable<T>
    {
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);

            var sink = new DoObserver<T>(observer, onNext, onError, onCompleted);
            sink.SetUpstream(source.Subscribe(sink));
            return sink;
        }
    }

    private sealed class DoObserver<T>(IObserver<T> downstream, Action<T> onNext, Action<Exception> onError, Action onCompleted) : RxContractSink<T, T>(downstream)
    {
        public override void OnNext(T value)
        {
            if (IsStopped)
            {
                return;
            }

            try
            {
                onNext(value);
            }
            catch (Exception ex)
            {
                ForwardOnError(ex);
                return;
            }

            // Outside the try: a downstream exception propagates to the producer.
            Downstream.OnNext(value);
        }

        public override void OnError(Exception error)
        {
            if (IsStopped)
            {
                return;
            }

            try
            {
                onError(error);
            }
            catch (Exception ex)
            {
                ForwardOnError(ex);
                return;
            }

            ForwardOnError(error);
        }

        public override void OnCompleted()
        {
            if (IsStopped)
            {
                return;
            }

            try
            {
                onCompleted();
            }
            catch (Exception ex)
            {
                ForwardOnError(ex);
                return;
            }

            ForwardOnCompleted();
        }
    }
}
#endif
