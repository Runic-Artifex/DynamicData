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
    /// Subscribes to the latest inner <c>IObservableList&lt;T&gt;</c>, switching to each new source and clearing the result when switching.
    /// This is the changeset-aware equivalent of Rx's <c>Observable.Switch&lt;TSource&gt;(IObservable&lt;IObservable&lt;TSource&gt;&gt;)</c>, which cannot be applied directly to changeset streams.
    /// </summary>
    /// <typeparam name="T">The type of the object.</typeparam>
    /// <param name="sources">An observable that emits <c>IObservableList&lt;T&gt;</c> instances. Each emission triggers a switch to the new list.</param>
    /// <returns>A list changeset stream reflecting the most recently received inner list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>Convenience overload that calls <c>Connect()</c> on each inner list, then delegates to <c>Switch&lt;T&gt;(IObservable&lt;IObservable&lt;IChangeSet&lt;T&gt;&gt;&gt;)</c>.</para>
    /// </remarks>
    /// <seealso><c>Switch&lt;T&gt;(IObservable&lt;IObservable&lt;IChangeSet&lt;T&gt;&gt;&gt;)</c></seealso>
    public static IObservable<IChangeSet<T>> Switch<T>(this IObservable<IObservableList<T>> sources)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(sources);

        return sources.Select(cache => cache.Connect()).Switch();
    }

    /// <summary>
    /// Subscribes to the latest inner changeset stream, switching to each new source and clearing the destination when switching.
    /// The previous subscription is disposed before the new inner stream is subscribed, and the result is cleared and repopulated from it.
    /// </summary>
    /// <typeparam name="T">The type of the object.</typeparam>
    /// <param name="sources">An <c>IObservable&lt;T&gt;</c> of <c>IObservable&lt;T&gt;</c> changeset streams. The operator subscribes to the latest inner stream.</param>
    /// <returns>A list changeset stream reflecting the most recently received inner changeset stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// <para>
    /// On switch, a <b>Clear</b> of the previous items is followed by the new source's changes. When the new source delivers its first non-empty
    /// changeset while it is being subscribed (as <c>Connect()</c> on a populated list does), the clear and that changeset reach
    /// subscribers as one changeset, whose indexes start from the cleared list. A new source that has not delivered by the time it
    /// is subscribed (delayed or empty) gets the clear on its own and its changesets when they arrive.
    /// A pending clear is also published on its own before an error or completion that ends the handoff.
    /// </para>
    /// <para>
    /// At most one inner source is subscribed at a time: the previous subscription is disposed before the replacement
    /// is subscribed, so an inner source that acquires a resource exclusively is never held concurrently across a switch.
    /// A source selected while the previous one is being subscribed or torn down starts once that has finished; only the
    /// latest selection is subscribed, and superseded sources contribute nothing.
    /// </para>
    /// <para><b>Worth noting:</b> Each switch clears the entire downstream list before populating from the new source. Subscribers see a full reset on every switch, and with a delayed new source they see the empty list until it delivers.</para>
    /// <para><b>Also worth noting:</b>This operator intentionally shadows the native <see cref="Observable.Switch{TSource}(IObservable{IObservable{TSource}})"/> operator, as downstream listeners will generally become corrupt when the native operator is used. This is due to its lack of the automatic-clearing behavior mentioned above.</para>
    /// </remarks>
    /// <seealso><c>Switch&lt;T&gt;(IObservable&lt;IObservableList&lt;T&gt;&gt;)</c></seealso>
    public static IObservable<IChangeSet<T>> Switch<T>(this IObservable<IObservable<IChangeSet<T>>> sources)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(sources);

        return new Switch<T>(sources).Run();
    }
}
