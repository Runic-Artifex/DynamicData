// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

namespace DynamicData.Internal;

/// <summary>
/// Runic (DD-044): base sink for the Primitives-flavour operator replacements that follow the
/// System.Reactive contract. Callback failures are delivered as <c>OnError</c>; an exception thrown
/// by the downstream <c>OnNext</c> propagates to the producer without terminating the sink, so an
/// <c>OnError</c> that an upstream operator converts it into is still forwarded.
/// </summary>
/// <typeparam name="TSource">The type of the upstream values.</typeparam>
/// <typeparam name="TResult">The type of the downstream values.</typeparam>
/// <param name="downstream">The downstream observer.</param>
internal abstract class RxContractSink<TSource, TResult>(IObserver<TResult> downstream) : IObserver<TSource>, IDisposable
{
    private static readonly IDisposable Disposed = Disposable.Empty;

    private IDisposable? _upstream;
    private volatile bool _stopped;

    protected IObserver<TResult> Downstream => downstream;

    protected bool IsStopped => _stopped;

    public void SetUpstream(IDisposable upstream)
    {
        if (Interlocked.CompareExchange(ref _upstream, upstream, null) is not null)
        {
            // Terminated or disposed during Subscribe.
            upstream.Dispose();
        }
    }

    public abstract void OnNext(TSource value);

    public virtual void OnError(Exception error) => ForwardOnError(error);

    public virtual void OnCompleted() => ForwardOnCompleted();

    public void Dispose()
    {
        _stopped = true;
        DisposeUpstream();
    }

    protected void ForwardOnError(Exception error)
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        try
        {
            downstream.OnError(error);
        }
        finally
        {
            DisposeUpstream();
        }
    }

    protected void ForwardOnCompleted()
    {
        if (_stopped)
        {
            return;
        }

        _stopped = true;
        try
        {
            downstream.OnCompleted();
        }
        finally
        {
            DisposeUpstream();
        }
    }

    private void DisposeUpstream() => Interlocked.Exchange(ref _upstream, Disposed)?.Dispose();
}
#endif
