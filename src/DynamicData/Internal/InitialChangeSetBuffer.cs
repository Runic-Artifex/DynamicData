// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM
namespace DynamicData.Reactive.Internal;
#else
namespace DynamicData.Internal;
#endif

/// <summary>Owns a single initial buffer window and its source subscription.</summary>
/// <typeparam name="TChangeSet">The changeset type.</typeparam>
internal static class InitialChangeSetBuffer<TChangeSet>
    where TChangeSet : IChangeSet
{
    /// <summary>Buffers from the first non-empty changeset, then forwards immediately.</summary>
    /// <param name="source">The source changesets.</param>
    /// <param name="duration">The initial window duration.</param>
    /// <param name="scheduler">The timer scheduler.</param>
    /// <param name="combine">Combines changesets without changing their order.</param>
    /// <returns>The buffered sequence.</returns>
    public static IObservable<TChangeSet> Run(IObservable<TChangeSet> source, TimeSpan duration, IScheduler scheduler, Func<IEnumerable<TChangeSet>, TChangeSet> combine) =>
        Observable.Create<TChangeSet>(observer =>
        {
            var queue = new DeliveryQueue<TChangeSet>(observer);
            var subscription = new SingleAssignmentDisposable();
            var timer = new SingleAssignmentDisposable();
            var buffer = new List<TChangeSet>();
            var buffering = duration > TimeSpan.Zero;
            var started = false;
            var stopped = false;

            void Flush(DeliveryQueue<TChangeSet>.ScopedAccess access)
            {
                buffering = false;
                if (buffer.Count != 0)
                {
                    var result = combine(buffer);
                    buffer = [];
                    access.EnqueueNext(result);
                }
            }

            void Dispose()
            {
                queue.Dispose();
                using (queue.AcquireLock())
                {
                    stopped = true;
                    buffer.Clear();
                }

                timer.Dispose();
                subscription.Dispose();
            }

            void Next(TChangeSet changes)
            {
                try
                {
                    var schedule = false;
                    using (var access = queue.AcquireLock())
                    {
                        if (stopped || changes.Count == 0)
                        {
                            return;
                        }

                        if (buffering)
                        {
                            buffer.Add(changes);
                            schedule = !started;
                            started = true;
                        }
                        else
                        {
                            access.EnqueueNext(changes);
                        }
                    }

                    if (schedule)
                    {
                        // Assignment after scheduling is safe even when a scheduler invokes
                        // synchronously, or completion/disposal cancels before assignment.
                        timer.Disposable = scheduler.Schedule(duration, () =>
                        {
                            try
                            {
                                using var access = queue.AcquireLock();
                                if (!stopped && buffering)
                                {
                                    Flush(access);
                                }
                            }
                            catch
                            {
                                Dispose();
                                throw;
                            }
                            finally
                            {
                                timer.Dispose();
                            }
                        });
                    }
                }
                catch
                {
                    Dispose();
                    throw;
                }
            }

            void Error(Exception error)
            {
                try
                {
                    using var access = queue.AcquireLock();
                    if (stopped)
                    {
                        return;
                    }

                    stopped = true;
                    buffer.Clear();
                    access.EnqueueError(error);
                }
                catch
                {
                    Dispose();
                    throw;
                }
                finally
                {
                    timer.Dispose();
                    subscription.Dispose();
                }
            }

            void Complete()
            {
                try
                {
                    using var access = queue.AcquireLock();
                    if (stopped)
                    {
                        return;
                    }

                    stopped = true;
                    Flush(access);
                    access.EnqueueCompleted();
                }
                catch
                {
                    Dispose();
                    throw;
                }
                finally
                {
                    timer.Dispose();
                    subscription.Dispose();
                }
            }

            try
            {
                subscription.Disposable = source.Subscribe(Next, Error, Complete);
            }
            catch
            {
                Dispose();
                throw;
            }

            return Disposable.Create(Dispose);
        });
}
