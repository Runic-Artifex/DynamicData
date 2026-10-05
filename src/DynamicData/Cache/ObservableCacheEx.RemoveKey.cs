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
    /// Strips the key from a cache changeset, converting <c>IChangeSet&lt;TObject, TKey&gt;</c> to
    /// <c>IChangeSet&lt;TObject&gt;</c> (list changeset). Cache keys identify each list position,
    /// keeping equal values and shared references under different keys distinct.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The source <c>IObservable&lt;IChangeSet&lt;TObject, TKey&gt;&gt;</c> to strip keys from, producing an unkeyed list changeset.</param>
    /// <returns>A list changeset stream without key information.</returns>
    /// <remarks>
    /// Each subscription starts with an empty projection and must observe its complete history. A populated
    /// cache's <c>Connect()</c> stream satisfies this contract by supplying its initial contents as additions.
    /// Positions are tracked independently per subscription; resubscribing starts again from empty.
    /// Additions use a supplied index within the projection, or append when the index is unspecified or outside
    /// its extent. Updates emit a removal at the key's tracked position followed by an addition at the supplied
    /// position, or at the end when unindexed. Removals use the key's tracked position. Refreshes retain the
    /// self-replacement reason and carry the same tracked previous and current index. Moves update the key's
    /// position and carry its actual previous and current indexes.
    /// <para>
    /// Partial histories, such as <c>Preview()</c> without preceding additions, and prepopulated target lists are
    /// outside this contract. Unknown keys and duplicate additions report unspecified indexes without changing
    /// tracked positions. An unknown move emits an unindexed removal and addition, because list moves require
    /// known indexes. These fallbacks do not promise correct binding when the original additions were omitted.
    /// </para>
    /// </remarks>
    /// <seealso><c>ObservableListEx.AddKey&lt;TObject, TKey&gt;(IObservable&lt;IChangeSet&lt;TObject&gt;&gt;, Func&lt;TObject, TKey&gt;)</c></seealso>
    /// <seealso><c>ChangeKey&lt;TObject, TSourceKey, TDestinationKey&gt;(IObservable&lt;IChangeSet&lt;TObject, TSourceKey&gt;&gt;, Func&lt;TObject, TDestinationKey&gt;)</c></seealso>
    public static IObservable<IChangeSet<TObject>> RemoveKey<TObject, TKey>(this IObservable<IChangeSet<TObject, TKey>> source)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return Observable.Defer(
            () =>
            {
                var positions = new KeyPositionIndex<TKey>();

                return source.Select(
                    changes =>
                    {
                        // Keep individual reasons and sequential indexes, including a Remove/Add pair for updates.
                        var result = new ChangeSet<TObject>(changes.Count + changes.Updates);
                        foreach (var change in changes.ToConcreteType())
                        {
                            switch (change.Reason)
                            {
                                case ChangeReason.Add:
                                    result.Add(new Change<TObject>(ListChangeReason.Add, change.Current, Insert(change.Key, change.CurrentIndex)));
                                    break;

                                case ChangeReason.Refresh:
                                    var index = positions.TryGetIndex(change.Key, out var located) ? located : -1;
                                    result.Add(new Change<TObject>(ListChangeReason.Replace, change.Current, change.Current, index, index));
                                    break;

                                case ChangeReason.Moved:
                                    var previousIndex = positions.Remove(change.Key);
                                    if (previousIndex < 0)
                                    {
                                        // A list Moved change rejects unknown indexes. Do not invent a tracked entry.
                                        result.Add(new Change<TObject>(ListChangeReason.Remove, change.Current));
                                        result.Add(new Change<TObject>(ListChangeReason.Add, change.Current));
                                    }
                                    else
                                    {
                                        result.Add(new Change<TObject>(change.Current, Insert(change.Key, change.CurrentIndex), previousIndex));
                                    }

                                    break;

                                case ChangeReason.Update:
                                    var removedIndex = positions.Remove(change.Key);
                                    result.Add(new Change<TObject>(ListChangeReason.Remove, change.Previous.Value, removedIndex));
                                    result.Add(new Change<TObject>(ListChangeReason.Add, change.Current,
                                        removedIndex < 0 ? -1 : Insert(change.Key, change.CurrentIndex)));
                                    break;

                                case ChangeReason.Remove:
                                    result.Add(new Change<TObject>(ListChangeReason.Remove, change.Current, positions.Remove(change.Key)));
                                    break;
                            }
                        }

                        return result;
                    });

                int Insert(TKey key, int suppliedIndex)
                {
                    if (positions.Contains(key))
                    {
                        return -1;
                    }

                    var index = suppliedIndex >= 0 && suppliedIndex <= positions.Count ? suppliedIndex : positions.Count;
                    positions.InsertAt(index, key);
                    return index;
                }
            });
    }

    /// <summary>
    /// Removes a specific key from the cache. Equivalent to <c>source.Edit(u =&gt; u.RemoveKey(key))</c>.
    /// </summary>
    /// <typeparam name="TObject">The type of the object.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="source">The <c>ISourceCache&lt;TObject, TKey&gt;</c> from which to remove a key.</param>
    /// <param name="key">The <typeparamref name="TKey"/> key to remove.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static void RemoveKey<TObject, TKey>(this ISourceCache<TObject, TKey> source, TKey key)
        where TObject : notnull
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        source.Edit(updater => updater.RemoveKey(key));
    }
}
