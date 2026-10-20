// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

// Runic (DD-044): deliberately in the root DynamicData namespace so that every
// source.DistinctUntilChanged(...) extension call in DynamicData.* code binds here instead of
// ReactiveUI.Primitives' DistinctUntilChanged (see ScanExtensions). The System.Reactive flavour
// keeps Observable.DistinctUntilChanged.
namespace DynamicData;

/// <summary>
/// Primitives-flavour replacement for <c>DistinctUntilChanged</c> that follows the System.Reactive contract.
/// </summary>
/// <remarks>
/// ReactiveUI.Primitives 8.4 and 9.0 <c>DistinctUntilChanged</c> throws comparer exceptions back to
/// the producer instead of delivering them as <c>OnError</c>, and disposes itself when a downstream
/// <c>OnNext</c> throws, so an <c>OnError</c> an upstream operator converted that exception into was
/// dropped. This implementation delivers comparer failures as <c>OnError</c> and lets downstream
/// exceptions propagate without terminating.
/// </remarks>
internal static class DistinctUntilChangedExtensions
{
    /// <summary>Emits only values that differ from the previous value, using the default comparer.</summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <returns>The values that differ from their predecessor.</returns>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source) =>
        source.DistinctUntilChanged(EqualityComparer<T>.Default);

    /// <summary>
    /// Emits only values that differ from the previous value. An exception thrown by the comparer is
    /// delivered as <c>OnError</c>.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="comparer">The comparer; <see langword="null"/> uses the default comparer.</param>
    /// <returns>The values that differ from their predecessor.</returns>
    public static IObservable<T> DistinctUntilChanged<T>(this IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        ArgumentNullException.ThrowIfNull(source);

        return new DistinctUntilChangedObservable<T>(source, comparer ?? EqualityComparer<T>.Default);
    }

    private sealed class DistinctUntilChangedObservable<T>(IObservable<T> source, IEqualityComparer<T> comparer) : IObservable<T>
    {
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);

            var sink = new DistinctUntilChangedObserver<T>(observer, comparer);
            sink.SetUpstream(source.Subscribe(sink));
            return sink;
        }
    }

    private sealed class DistinctUntilChangedObserver<T>(IObserver<T> downstream, IEqualityComparer<T> comparer) : RxContractSink<T, T>(downstream)
    {
        private bool _hasLast;
        private T? _last;

        public override void OnNext(T value)
        {
            if (IsStopped)
            {
                return;
            }

            bool unchanged;
            try
            {
                unchanged = _hasLast && comparer.Equals(_last!, value);
            }
            catch (Exception ex)
            {
                ForwardOnError(ex);
                return;
            }

            if (unchanged)
            {
                return;
            }

            _hasLast = true;
            _last = value;

            // Outside the try: a downstream exception propagates to the producer.
            Downstream.OnNext(value);
        }
    }
}
#endif
