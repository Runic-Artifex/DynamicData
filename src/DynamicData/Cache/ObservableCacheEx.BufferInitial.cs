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
/// Extensions for dynamic data.
/// </summary>
public static partial class ObservableCacheEx
{
    /// <summary>
    /// Buffers the initial burst of changesets for the specified duration, merges them into a single
    /// changeset, then passes all subsequent changesets through without buffering.
    /// </summary>
    /// <typeparam name="TObject">The object type.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;</c> to buffer during the initial loading period.</param>
    /// <param name="initialBuffer">The <see cref="TimeSpan"/> non-negative time window to buffer, measured from the first non-empty changeset. Zero forwards immediately.</param>
    /// <param name="scheduler">The scheduler for timing. Defaults to <see cref="GlobalConfig.DefaultScheduler"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialBuffer"/> is negative.</exception>
    /// <returns>An observable that emits one merged changeset for the initial burst, then passthrough for the rest.</returns>
    /// <remarks>
    /// <para>
    /// Useful for aggregating the initial snapshot (which may arrive as many small changesets) into a
    /// single changeset for efficient downstream processing, while leaving subsequent live updates untouched.
    /// </para>
    /// <para>Empty changesets are suppressed, matching <c>DeferUntilLoaded</c>. Completion flushes any pending changes once; an error discards them. Disposal cancels the timer and source subscription.</para>
    /// </remarks>
    /// <seealso><c>Batch&lt;TObject, TKey&gt;</c></seealso>
    /// <seealso><c>DeferUntilLoaded&lt;TObject, TKey&gt;(IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;)</c></seealso>
    public static IObservable<IChangeSet<TObject, TKey>> BufferInitial<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source, TimeSpan initialBuffer, IScheduler? scheduler = null)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        if (initialBuffer < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(initialBuffer), initialBuffer, "The initial buffer duration must be non-negative.");
        }

        return InitialChangeSetBuffer<IChangeSet<TObject, TKey>>.Run(
            source,
            initialBuffer,
            scheduler ?? GlobalConfig.DefaultScheduler,
            updates => new ChangeSet<TObject, TKey>(updates.SelectMany(update => update)));
    }
}
