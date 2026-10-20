// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if !REACTIVE_SHIM

namespace DynamicData.Internal;

/// <summary>
/// Runic: task bridges that ReactiveUI.Primitives does not provide, matching the System.Reactive operators
/// that the System.Reactive flavour uses in their place.
/// </summary>
internal static class TaskBridge
{
    /// <summary>
    /// Observes a task started for each subscription, like System.Reactive's
    /// <c>Observable.FromAsync(Func&lt;CancellationToken, Task&gt;, new TaskObservationOptions(ImmediateScheduler.Instance, ignoreExceptionsAfterUnsubscribe: true))</c>.
    /// </summary>
    /// <param name="taskFactory">Starts the task. Its token is cancelled when the subscription is disposed.</param>
    /// <returns>
    /// A signal that emits one <see cref="Unit"/> and completes when the task succeeds, fails with the task's exception
    /// (a <see cref="TaskCanceledException"/> when it is cancelled) or with the exception <paramref name="taskFactory"/>
    /// throws. The outcome is delivered on the thread that completes the task, and nothing is delivered after disposal.
    /// </returns>
    public static IObservable<Unit> FromAsync(Func<CancellationToken, Task> taskFactory)
    {
        ArgumentNullException.ThrowIfNull(taskFactory);

        return Observable.Create<Unit>(
            observer =>
            {
                var subscription = new TaskSubscription(observer);

                Task task;
                try
                {
                    task = taskFactory(subscription.Token);
                }
                catch (Exception ex)
                {
                    observer.OnError(ex);
                    return subscription;
                }

                // ExecuteSynchronously delivers on the thread that completes the task, or inline when it is already
                // complete, whatever SynchronizationContext that thread has.
                task.ContinueWith(
                    static (completed, state) => ((TaskSubscription)state!).Deliver(completed),
                    subscription,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);

                return subscription;
            });
    }

    /// <summary>
    /// Invokes <paramref name="onNext"/> for each value of <paramref name="source"/>, like System.Reactive's
    /// <c>ForEachAsync(Action&lt;T&gt;, CancellationToken)</c>.
    /// </summary>
    /// <typeparam name="T">The type of the values.</typeparam>
    /// <param name="source">The source to observe.</param>
    /// <param name="onNext">The action to invoke for each value.</param>
    /// <param name="cancellationToken">Unsubscribes from <paramref name="source"/> and cancels the task.</param>
    /// <returns>
    /// A task that completes when <paramref name="source"/> completes and fails when it fails or
    /// <paramref name="onNext"/> throws. Its continuations are not forced onto another thread.
    /// </returns>
    public static Task ForEachAsync<T>(this IObservable<T> source, Action<T> onNext, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(onNext);

        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        var loop = new ForEachLoop<T>(onNext);
        loop.Start(source, cancellationToken);
        return loop.Task;
    }

    private sealed class TaskSubscription(IObserver<Unit> observer) : IDisposable
    {
        // Cancelled but not disposed, like System.Reactive's CancellationDisposable: the task may still use the token.
        [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The task may still observe the token after disposal.")]
        private readonly CancellationTokenSource _cancellation = new();
        private int _disposed;

        public CancellationToken Token => _cancellation.Token;

        public void Deliver(Task task)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            if (task.IsFaulted)
            {
                observer.OnError(task.Exception!.InnerException ?? task.Exception);
            }
            else if (task.IsCanceled)
            {
                observer.OnError(new TaskCanceledException(task));
            }
            else
            {
                observer.OnNext(Unit.Default);
                observer.OnCompleted();
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _cancellation.Cancel();
            }
        }
    }

    [SuppressMessage("Design", "CA1001:Types that own disposable fields should be disposable", Justification = "The loop disposes its subscription when it ends or is cancelled.")]
    private sealed class ForEachLoop<T>(Action<T> onNext) : IObserver<T>
    {
        // Continuations run synchronously on the thread that ends the loop, as with System.Reactive's ForEachAsync.
        private readonly TaskCompletionSource _completion = new();
        private readonly SingleAssignmentDisposable _subscription = new();
        private CancellationTokenRegistration _registration;

        public Task Task => _completion.Task;

        public void Start(IObservable<T> source, CancellationToken cancellationToken)
        {
            _registration = cancellationToken.Register(
                static state =>
                {
                    var (loop, token) = ((ForEachLoop<T>, CancellationToken))state!;
                    if (loop._completion.TrySetCanceled(token))
                    {
                        loop._subscription.Dispose();
                    }
                },
                (this, cancellationToken));

            try
            {
                _subscription.Disposable = source.Subscribe(this);
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        public void OnNext(T value)
        {
            if (_completion.Task.IsCompleted)
            {
                return;
            }

            try
            {
                onNext(value);
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        public void OnError(Exception error)
        {
            if (_completion.TrySetException(error))
            {
                Stop();
            }
        }

        public void OnCompleted()
        {
            if (_completion.TrySetResult())
            {
                Stop();
            }
        }

        private void Stop()
        {
            _registration.Dispose();
            _subscription.Dispose();
        }
    }
}
#endif
