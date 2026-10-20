// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace DynamicData.Tests.Domain;

/// <summary>
/// An item that owns observable children in every shape the many-to-one operators accept, so a test can
/// write into one child while a notification caused by another is being delivered.
/// </summary>
public sealed class FeedbackOwner : IDisposable
{
    public FeedbackOwner(string name, params Person[] children)
    {
        Name = name;
        Children.AddOrUpdate(children);
        ListChildren.AddRange(children);
        Collection = new ObservableCollection<Person>(children);
    }

    public string Name { get; }

    public SourceCache<Person, string> Children { get; } = new(static p => p.Name);

    public SourceList<Person> ListChildren { get; } = new();

    public ObservableCollection<Person> Collection { get; }

    /// <summary>Gets a value that replays its latest state to each new subscriber.</summary>
    public StateSignal<int> Value { get; } = new(0);

    /// <summary>Adds a child to every child collection.</summary>
    public void Add(Person child)
    {
        Children.AddOrUpdate(child);
        ListChildren.Add(child);
        Collection.Add(child);
    }

    public void Dispose()
    {
        Children.Dispose();
        ListChildren.Dispose();
        Value.Dispose();
    }

    public override string ToString() => Name;
}
