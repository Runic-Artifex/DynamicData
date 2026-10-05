// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

using DynamicData.Reactive.Cache.Internal;
#else

using DynamicData.Cache.Internal;
#endif

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
    /// Provides an overload of <c>Switch</c> for the supplied arguments.
    /// </summary>
    /// <typeparam name="TObject">The type of the TObject value.</typeparam>
    /// <typeparam name="TKey">The type of the TKey value.</typeparam>
    /// <param name="sources">An observable that emits <c>IObservableCache&lt;TObject, TKey&gt;</c> instances.</param>
    /// <returns>The resulting observable sequence.</returns>
    /// <remarks>Overload that accepts observable caches. Internally calls <c>Connect()</c> on each cache and delegates to the changeset overload.</remarks>
    public static IObservable<IChangeSet<TObject, TKey>> Switch<TObject, TKey>(this IObservable<IObservableCache<TObject, TKey>> sources)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return sources.Select(cache => cache.Connect()).Switch();
    }

    /// <summary>
    /// Switches virtualized streams using keyed cache clearing on each source change.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="sources">An observable of virtualized changeset streams.</param>
    /// <returns>Base keyed changesets from the selected source, with removals on each switch.</returns>
    /// <remarks>
    /// This overload returns the base keyed type, so viewport ordering and <see cref="VirtualContext{TObject}"/>
    /// are not guaranteed on every batch. Incoming batches may retain their runtime context type, but
    /// clearing batches contain no viewport context. Switch the keyed source before each presentation's
    /// SortAndVirtualize when context is needed. Context-preserving switching requires a separate contract.
    /// </remarks>
    public static IObservable<IChangeSet<TObject, TKey>> Switch<TObject, TKey>(this IObservable<IObservable<IChangeSet<TObject, TKey, VirtualContext<TObject>>>> sources)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return ObservableCacheEx.Switch((IObservable<IObservable<IChangeSet<TObject, TKey>>>)sources);
    }

    /// <summary>
    /// Subscribes to the latest inner changeset stream, unsubscribing from the previous one on each switch.
    /// When switching, the old source's items are removed and the new source's items are added.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="sources">An <c>IObservable&lt;T&gt;</c> of <c>IObservable&lt;T&gt;</c> changeset streams. The operator subscribes to the latest inner stream.</param>
    /// <returns>A changeset stream reflecting the items from the most recently emitted inner source.</returns>
    /// <remarks>
    /// <para>On switch: <b>Remove</b> is emitted for all items from the previous source, then <b>Add</b> for all items from the new source.</para>
    /// <para>
    /// At most one inner source is subscribed at a time: the previous subscription is disposed before the replacement
    /// is subscribed, so an inner source that acquires a resource exclusively is never held concurrently across a switch.
    /// </para>
    /// <para><b>Worth noting:</b> Each switch clears the entire downstream cache before populating from the new source. Subscribers see a full remove-then-add reset on every switch.</para>
    /// <para><b>Also worth noting:</b>This operator intentionally shadows the native <see cref="Observable.Switch{TSource}(IObservable{IObservable{TSource}})"/> operator, as downstream listeners will generally become corrupt when the native operator is used. This is due to its lack of the automatic-clearing behavior mentioned above.</para>
    /// </remarks>
    public static IObservable<IChangeSet<TObject, TKey>> Switch<TObject, TKey>(this IObservable<IObservable<IChangeSet<TObject, TKey>>> sources)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);

        return new Switch<TObject, TKey>(sources).Run();
    }
}
