// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the BufferIf class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="pauseIfTrueSelector">The pauseIfTrueSelector value.</param>
/// <param name="initialPauseState">The initialPauseState value.</param>
/// <param name="timeOut">The timeOut value.</param>
/// <param name="scheduler">The scheduler value.</param>
internal sealed class BufferIf<T>(IObservable<IChangeSet<T>> source, IObservable<bool> pauseIfTrueSelector, bool initialPauseState = false, TimeSpan? timeOut = null, IScheduler? scheduler = null)
    where T : notnull
{
    /// <summary>
    /// The _pauseIfTrueSelector field.
    /// </summary>
    private readonly IObservable<bool> _pauseIfTrueSelector = pauseIfTrueSelector ?? throw new ArgumentNullException(nameof(pauseIfTrueSelector));

    /// <summary>
    /// The _scheduler field.
    /// </summary>
    private readonly IScheduler _scheduler = scheduler ?? GlobalConfig.DefaultScheduler;

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly IObservable<IChangeSet<T>> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// The _timeOut field.
    /// </summary>
    private readonly TimeSpan _timeOut = timeOut ?? TimeSpan.Zero;

    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IChangeSet<T>> Run() => Observable.Create<IChangeSet<T>>(observer =>
    {
        var queue = new SharedDeliveryQueue();
        var subscriptions = new CompositeDisposable();
        var paused = initialPauseState;
        var stopped = false;
        var buffer = new ChangeSet<T>();
        var timeoutSubscriber = new SerialDisposable();
        var timeoutSubject = new Signal<bool>();
        subscriptions.Add(timeoutSubscriber);

        void Flush()
        {
            if (buffer.Count == 0)
            {
                return;
            }

            var changes = buffer;
            buffer = [];
            observer.OnNext(changes);
        }

        void Fail(Exception error)
        {
            if (stopped)
            {
                return;
            }

            stopped = true;
            buffer.Clear();
            observer.OnError(error);
        }

        var bufferSelector = Observable.Return(initialPauseState)
            .Concat(_pauseIfTrueSelector.DeliveryQueueMerge(timeoutSubject))
            .ObserveOn(_scheduler).SynchronizeSafe(queue);

        subscriptions.Add(bufferSelector.Subscribe(state =>
        {
            if (stopped)
            {
                return;
            }

            paused = state;
            timeoutSubscriber.Disposable = Disposable.Empty;
            if (!paused)
            {
                Flush();
            }
            else if (_timeOut != TimeSpan.Zero)
            {
                timeoutSubscriber.Disposable = Observable.Timer(_timeOut, _scheduler)
                    .Select(static _ => false).SubscribeSafe(timeoutSubject);
            }
        }, Fail));

        if (!stopped)
        {
            subscriptions.Add(_source.SynchronizeSafe(queue).Subscribe(updates =>
            {
                if (stopped)
                {
                    return;
                }

                if (paused)
                {
                    buffer.AddRange(updates);
                }
                else
                {
                    observer.OnNext(updates);
                }
            }, Fail, () =>
            {
                if (stopped)
                {
                    return;
                }

                stopped = true;
                Flush();
                observer.OnCompleted();
            }));
        }

        return Disposable.Create(() =>
        {
            queue.Dispose();
            subscriptions.Dispose();
            timeoutSubject.Dispose();
            buffer.Clear();
        });
    });
}
