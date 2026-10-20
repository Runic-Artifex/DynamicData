// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

// Runic (DD-044): this file is deliberately in the root DynamicData namespace. Extension lookup
// searches enclosing namespaces before the compilation unit's global usings, so every
// source.Scan(seed, accumulator) call in DynamicData.* code binds to this method instead of
// ReactiveUI.Primitives' Scan, without changing upstream call sites. The System.Reactive flavour
// keeps Observable.Scan, which already has these semantics.
namespace DynamicData;

/// <summary>
/// Primitives-flavour replacement for <c>Scan</c> that follows the System.Reactive contract.
/// </summary>
/// <remarks>
/// ReactiveUI.Primitives 8.4 and 9.0 <c>Scan</c> does not catch accumulator exceptions: they are
/// thrown back to the producer instead of being delivered as <c>OnError</c>. It also disposes
/// itself when a downstream <c>OnNext</c> throws, so when an upstream <c>SubscribeSafe</c> (as in
/// <c>SourceList</c>/<c>SourceCache</c> <c>Connect</c>) converts that exception to <c>OnError</c>,
/// the error reaches a disposed operator and is silently dropped. This implementation delivers
/// accumulator failures as <c>OnError</c> and lets downstream exceptions propagate without
/// terminating itself, as System.Reactive does.
/// </remarks>
internal static class ScanExtensions
{
    /// <summary>
    /// Applies an accumulator over the source and emits each intermediate result.
    /// An exception thrown by the accumulator is delivered as <c>OnError</c>.
    /// </summary>
    /// <typeparam name="TSource">The type of the source values.</typeparam>
    /// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The accumulator.</param>
    /// <returns>An observable of accumulated values.</returns>
    public static IObservable<TAccumulate> Scan<TSource, TAccumulate>(this IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(accumulator);

        return new ScanObservable<TSource, TAccumulate>(source, seed, accumulator);
    }

    private sealed class ScanObservable<TSource, TAccumulate>(IObservable<TSource> source, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator) : IObservable<TAccumulate>
    {
        public IDisposable Subscribe(IObserver<TAccumulate> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);

            var sink = new ScanObserver<TSource, TAccumulate>(observer, seed, accumulator);
            sink.SetUpstream(source.Subscribe(sink));
            return sink;
        }
    }

    private sealed class ScanObserver<TSource, TAccumulate>(IObserver<TAccumulate> downstream, TAccumulate seed, Func<TAccumulate, TSource, TAccumulate> accumulator) : IObserver<TSource>, IDisposable
    {
        private static readonly IDisposable Disposed = Disposable.Empty;

        private TAccumulate _accumulate = seed;
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

        public void OnNext(TSource value)
        {
            if (_stopped)
            {
                return;
            }

            TAccumulate next;
            try
            {
                next = accumulator(_accumulate, value);
            }
            catch (Exception ex)
            {
                _stopped = true;
                downstream.OnError(ex);
                DisposeUpstream();
                return;
            }

            _accumulate = next;

            // Outside the try: a downstream exception propagates to the producer and leaves this
            // operator able to forward an OnError that an upstream SubscribeSafe converts it into.
            downstream.OnNext(next);
        }

        public void OnError(Exception error)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            downstream.OnError(error);
            DisposeUpstream();
        }

        public void OnCompleted()
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            downstream.OnCompleted();
            DisposeUpstream();
        }

        public void Dispose()
        {
            _stopped = true;
            DisposeUpstream();
        }

        private void DisposeUpstream() => Interlocked.Exchange(ref _upstream, Disposed)?.Dispose();
    }
}
#endif
