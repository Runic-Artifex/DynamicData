// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

// ReSharper disable once CheckNamespace
#if REACTIVE_SHIM
namespace DynamicData.Reactive;
#else
namespace DynamicData;
#endif

/// <summary>
/// Extensions for ObservableList.
/// </summary>
public static partial class ObservableListEx
{
    /// <summary>
    /// Collects change sets in each time window and concatenates their changes in source order.
    /// </summary>
    /// <typeparam name="TObject">The type of the items.</typeparam>
    /// <param name="source">The source change sets.</param>
    /// <param name="timeSpan">The time window for batching.</param>
    /// <param name="scheduler">The scheduler for timing. Defaults to <see cref="GlobalConfig.DefaultScheduler"/>.</param>
    /// <returns>An observable emitting combined change sets for nonempty buffers.</returns>
    /// <remarks>
    /// Uses Buffer followed by FlattenBufferResult. Completion flushes remaining changes;
    /// errors discard the pending buffer and propagate immediately. Disposal discards pending changes.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeSpan"/> is negative.</exception>
    public static IObservable<IChangeSet<TObject>> Batch<TObject>(this IObservable<IChangeSet<TObject>> source, TimeSpan timeSpan, IScheduler? scheduler = null)
        where TObject : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        if (timeSpan < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeSpan), timeSpan, "The batch window must not be negative.");
        }

        return source.Buffer(timeSpan, scheduler ?? GlobalConfig.DefaultScheduler).FlattenBufferResult();
    }
}
