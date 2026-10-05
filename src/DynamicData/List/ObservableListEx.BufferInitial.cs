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
    /// Buffers changesets during an initial time window, then emits a single combined changeset and passes through subsequent changes.
    /// </summary>
    /// <typeparam name="TObject">The type of items in the list.</typeparam>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject&gt;&gt;</c> to buffer during the initial loading period.</param>
    /// <param name="initialBuffer">The <see cref="TimeSpan"/> non-negative time period measured from the first non-empty changeset. Zero forwards immediately.</param>
    /// <param name="scheduler">The <see cref="IScheduler"/> for timing the buffer window.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="initialBuffer"/> is negative.</exception>
    /// <returns>A list changeset stream where the initial burst is combined into one changeset.</returns>
    /// <remarks>
    /// <para>
    /// For a configured duration after the first emission, all changesets are buffered and combined into a single emission.
    /// After this initial window, subsequent changesets pass through immediately. Empty changesets are suppressed, matching <c>DeferUntilLoaded</c>. Completion flushes pending changes once; an error discards them. Disposal cancels the timer and source subscription.
    /// </para>
    /// </remarks>
    /// <seealso><c>DeferUntilLoaded&lt;T&gt;(IObservable&lt;IChangeSet&lt;T&gt;&gt;)</c></seealso>
    /// <seealso><c>FlattenBufferResult&lt;T&gt;(IObservable&lt;IList&lt;IChangeSet&lt;T&gt;&gt;&gt;)</c></seealso>
    public static IObservable<IChangeSet<TObject>> BufferInitial<TObject>(this IObservable<IChangeSet<TObject>> source, TimeSpan initialBuffer, IScheduler? scheduler = null)
        where TObject : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        if (initialBuffer < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(initialBuffer), initialBuffer, "The initial buffer duration must be non-negative.");
        }

        return InitialChangeSetBuffer<IChangeSet<TObject>>.Run(
            source,
            initialBuffer,
            scheduler ?? GlobalConfig.DefaultScheduler,
            updates => new ChangeSet<TObject>(updates.SelectMany(update => update)));
    }
}
