// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

// Runic (DD-044): deliberately in the root DynamicData namespace so that every
// source.SubscribeSafe(observer) extension call in DynamicData.* code binds here instead of
// ReactiveUI.Primitives' SubscribeSafe (see ScanExtensions). Static
// PrimitivesLinqExtensions.SubscribeSafe(...) calls are unaffected; they are shared by both
// flavours. The System.Reactive flavour keeps System.ObservableExtensions.SubscribeSafe.
namespace DynamicData;

/// <summary>
/// Primitives-flavour replacement for the <c>SubscribeSafe(observer)</c> extension that follows the
/// System.Reactive contract.
/// </summary>
/// <remarks>
/// ReactiveUI.Primitives 8.4 and 9.0 <c>SubscribeSafe</c> catches an exception thrown by the
/// observer's <c>OnNext</c> and delivers it to the same observer as <c>OnError</c>. System.Reactive's
/// <c>SubscribeSafe</c> only guards subscription, so a subscriber's exception reaches the code that
/// produced the value (for example the <c>SourceList</c>/<c>SourceCache</c> edit). The conversion also
/// lost the exception when it reached an upstream Primitives operator that had already disposed
/// itself. This implementation keeps Primitives' terminal handling (one terminal notification, then
/// the upstream subscription is released). When the observer's <c>OnNext</c> throws, it releases the
/// upstream subscription and lets the exception propagate, as System.Reactive's subscription
/// safeguard does.
/// </remarks>
internal static class SubscribeSafeExtensions
{
    /// <summary>
    /// Subscribes the observer to the source. An exception thrown by the observer's <c>OnNext</c>
    /// ends the subscription and propagates to the producer.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="observer">The observer.</param>
    /// <returns>A disposable that ends the subscription.</returns>
    public static IDisposable SubscribeSafe<T>(this IObservable<T> source, IObserver<T> observer)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(observer);

        var sink = new SubscribeSafeObserver<T>(observer);
        sink.SetUpstream(source.Subscribe(sink));
        return sink;
    }

    private sealed class SubscribeSafeObserver<T>(IObserver<T> downstream) : RxContractSink<T, T>(downstream)
    {
        public override void OnNext(T value)
        {
            if (IsStopped)
            {
                return;
            }

            try
            {
                Downstream.OnNext(value);
            }
            catch
            {
                // Like System.Reactive's subscription safeguard: an observer that throws is detached
                // from the source before the exception propagates to the producer.
                Dispose();
                throw;
            }
        }
    }
}
#endif
