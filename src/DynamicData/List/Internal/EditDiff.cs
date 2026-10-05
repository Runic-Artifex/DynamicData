// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if REACTIVE_SHIM

namespace DynamicData.Reactive.List.Internal;
#else

namespace DynamicData.List.Internal;
#endif

/// <summary>
/// Provides members for the EditDiff class.
/// </summary>
/// <typeparam name="T">The type of the T value.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="equalityComparer">The equalityComparer value.</param>
internal sealed class EditDiff<T>(ISourceList<T> source, IEqualityComparer<T>? equalityComparer)
    where T : notnull
{
    /// <summary>
    /// The _equalityComparer field.
    /// </summary>
    private readonly IEqualityComparer<T> _equalityComparer = equalityComparer ?? EqualityComparer<T>.Default;

    /// <summary>
    /// The _source field.
    /// </summary>
    private readonly ISourceList<T> _source = source ?? throw new ArgumentNullException(nameof(source));

    /// <summary>
    /// Executes the Edit operation.
    /// </summary>
    /// <param name="items">The items value.</param>
    public void Edit(IEnumerable<T> items) => _source.Edit(
            innerList =>
            {
                var originalItems = innerList.AsArray();
                var newItems = items.AsArray();

                var available = new Dictionary<T, int>(_equalityComparer);
                foreach (var item in originalItems)
                {
                    available.TryGetValue(item, out var count);
                    available[item] = count + 1;
                }

                var retained = new Dictionary<T, int>(_equalityComparer);
                var adds = new List<T>();
                foreach (var item in newItems)
                {
                    if (available.TryGetValue(item, out var count) && count > 0)
                    {
                        available[item] = count - 1;
                        retained.TryGetValue(item, out var retainedCount);
                        retained[item] = retainedCount + 1;
                    }
                    else
                    {
                        adds.Add(item);
                    }
                }

                // Keep the earliest matching originals in their existing order;
                // append unmatched incoming occurrences in their input order.
                var index = 0;
                foreach (var item in originalItems)
                {
                    if (retained.TryGetValue(item, out var count) && count > 0)
                    {
                        retained[item] = count - 1;
                        index++;
                    }
                    else
                    {
                        innerList.RemoveAt(index);
                    }
                }

                innerList.AddRange(adds);
            });
}
