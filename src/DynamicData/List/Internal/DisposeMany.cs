// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the DisposeMany class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="source">The source value.</param>
internal sealed class DisposeMany<T>(IObservable<IChangeSet<T>> source)
    where T : notnull
{
    /// <summary>
    /// Executes the Run operation.
    /// </summary>
    /// <returns>The result of the operation.</returns>
    public IObservable<IChangeSet<T>> Run()
        => Observable.Create<IChangeSet<T>>(observer =>
        {
            var cachedItems = new List<T>();

            var sourceSubscription = source
                .SynchronizeSafe()
                .SubscribeSafe(Observer.Create<IChangeSet<T>>(
                    onNext: changeSet =>
                    {
                        // Track live ownership before delivery: downstream can dispose or reenter
                        // the subscription from OnNext, and teardown must see the current items.
                        cachedItems.Clone(changeSet);
                        try
                        {
                            observer.OnNext(changeSet);
                        }
                        finally
                        {
                            foreach (var change in changeSet)
                            {
                                switch (change.Reason)
                                {
                                    case ListChangeReason.Clear:
                                        foreach (var item in change.Range)
                                        {
                                            (item as IDisposable)?.Dispose();
                                        }

                                        break;

                                    case ListChangeReason.Remove:
                                        (change.Item.Current as IDisposable)?.Dispose();
                                        break;

                                    case ListChangeReason.RemoveRange:
                                        foreach (var item in change.Range)
                                        {
                                            (item as IDisposable)?.Dispose();
                                        }

                                        break;

                                    case ListChangeReason.Replace:
                                        if (change.Item.Previous.HasValue)
                                        {
                                            (change.Item.Previous.Value as IDisposable)?.Dispose();
                                        }

                                        break;
                                }
                            }
                        }
                    },
                    onError: error =>
                    {
                        try
                        {
                            observer.OnError(error);
                        }
                        finally
                        {
                            ProcessFinalization(cachedItems);
                        }
                    },
                    onCompleted: () =>
                    {
                        try
                        {
                            observer.OnCompleted();
                        }
                        finally
                        {
                            ProcessFinalization(cachedItems);
                        }
                    }));

            return Disposable.Create(() =>
            {
                sourceSubscription.Dispose();

                ProcessFinalization(cachedItems);
            });
        });

    /// <summary>
    /// Executes the ProcessFinalization operation.
    /// </summary>
    /// <param name="cachedItems">The cachedItems value.</param>
    private static void ProcessFinalization(List<T> cachedItems)
    {
        var remaining = cachedItems.ToArray();
        cachedItems.Clear();
        foreach (var item in remaining)
        {
            (item as IDisposable)?.Dispose();
        }
    }
}
