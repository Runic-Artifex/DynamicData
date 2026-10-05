using System.Collections.ObjectModel;

#if REACTIVE_ACCEPTANCE
using DynamicData.Reactive;
using DynamicData.Reactive.Aggregation;
using DynamicData.Reactive.Binding;
using System.Reactive.Linq;
#else
using DynamicData;
using DynamicData.Aggregation;
using DynamicData.Binding;
using ReactiveUI.Primitives;
#endif

// This is deliberately an ordinary application namespace. Its extension
// resolution, package graph and ownership rules are those of a Runic consumer.
namespace Runic.DynamicData.Acceptance;

internal static class Program
{
    private static int Main()
    {
        var cases = new (string Name, Action Run)[]
        {
            ("live presentation switches keyed feeds before virtualization and refreshes aggregates", LivePresentation),
            ("child failure closes its session and a new session recovers", ChildFailureRecovery),
            ("view close releases transformed rows and all control subscriptions", ViewCloseOwnership),
        };
        var failures = 0;
        foreach (var test in cases)
        {
            try
            {
                test.Run();
                Console.WriteLine($"PASS {test.Name}");
            }
            catch (Exception error)
            {
                failures++;
                Console.Error.WriteLine($"FAIL {test.Name}: {error}");
            }
        }

        return failures == 0 ? 0 : 1;
    }

    private static void LivePresentation()
    {
        using var first = new SourceCache<WorkRow, int>(row => row.Id);
        using var second = new SourceCache<WorkRow, int>(row => row.Id);
        using var empty = new SourceCache<WorkRow, int>(row => row.Id);
        var firstRows = new[] { new WorkRow(1, 20, 10), new WorkRow(2, 10, 20), new WorkRow(3, 30, 30) };
        var secondRows = new[] { new WorkRow(1, 15, 40), new WorkRow(4, 5, 60), new WorkRow(5, 25, 100) };
        first.AddOrUpdate(firstRows);
        second.AddOrUpdate(secondRows);

        using var selected = new CurrentSignal<IObservable<IChangeSet<WorkRow, int>>>(first.Connect());
        using var requests = new CurrentSignal<IVirtualRequest>(new VirtualRequest(0, 2));
        var shared = selected.Switch().AutoRefreshOnObservable(row => row.Invalidated).Publish();
        VirtualContext<WorkRow>? context = null;
        int? sum = null;
        double? average = null;
        int? minimum = null;
        int? maximum = null;
        using var viewport = shared.SortAndVirtualize(WorkRow.ByRank, requests)
            .Do(changes => context = changes.Context)
            .Bind(out ReadOnlyObservableCollection<WorkRow> visible)
            .Subscribe(_ => { }, Fail);
        using var sumSubscription = shared.Sum(row => row.Amount).Subscribe(value => sum = value, Fail);
        using var averageSubscription = shared.Avg(row => row.Amount, -1).Subscribe(value => average = value, Fail);
        using var minimumSubscription = shared.Minimum(row => row.Amount, -1).Subscribe(value => minimum = value, Fail);
        using var maximumSubscription = shared.Maximum(row => row.Amount, -1).Subscribe(value => maximum = value, Fail);
        using var connection = shared.Connect();

        AssertPresentation(visible, context, [2, 1], total: 3, "first selected feed");
        AssertAggregates(sum, average, minimum, maximum, 60, 20, 10, 30, "first selected feed");

        // Switch the plain keyed feed first. The contextual Switch overload
        // intentionally returns base changesets, so putting virtualization
        // after Switch is what gives this presentation current metadata.
        selected.OnNext(second.Connect());
        AssertPresentation(visible, context, [4, 1], total: 3, "second selected feed");
        AssertAggregates(sum, average, minimum, maximum, 200, 200D / 3D, 40, 100, "second selected feed");

        secondRows[2].Rank = 1;
        secondRows[2].Amount = 80;
        secondRows[2].Invalidated.OnNext(default);
        AssertPresentation(visible, context, [5, 4], total: 3, "refresh-driven reordering");
        AssertAggregates(sum, average, minimum, maximum, 180, 60, 40, 80, "refresh-driven aggregates");

        secondRows[0].Amount = 70;
        second.Edit(updater =>
        {
            updater.Refresh(secondRows[0]);
            updater.RemoveKey(4);
            updater.AddOrUpdate(new WorkRow(6, 12, 50));
        });
        AssertPresentation(visible, context, [5, 6], total: 3, "mixed mutable transaction");
        AssertAggregates(sum, average, minimum, maximum, 200, 200D / 3D, 50, 80, "mixed mutable transaction");

        requests.OnNext(new VirtualRequest(0, 0));
        AssertPresentation(visible, context, [], total: 3, "zero-sized presentation");
        selected.OnNext(empty.Connect());
        AssertPresentation(visible, context, [], total: 0, "empty selected feed");
        AssertAggregates(sum, average, minimum, maximum, 0, -1, -1, -1, "empty selected feed");

        var stableSum = sum;
        firstRows[0].Amount = 999;
        firstRows[0].Invalidated.OnNext(default);
        first.AddOrUpdate(new WorkRow(9, 0, 999));
        Require(sum == stableSum && visible.Count == 0, "A superseded feed changed the current presentation.");
        Require(firstRows.All(row => row.Invalidated.Subscribers == 0), "A superseded feed retained row invalidations.");
    }

    private static void ChildFailureRecovery()
    {
        using var source = new SourceCache<WorkRow, int>(row => row.Id);
        var first = new WorkRow(1, 1, 1);
        var second = new WorkRow(2, 2, 2);
        Exception? failure = null;
        var delivered = new List<string>();
        using var session = source.Connect().MergeMany(row => row.Results).Subscribe(delivered.Add, error => failure = error);
        source.AddOrUpdate([first, second]);
        Require(first.Results.Subscribers == 1 && second.Results.Subscribers == 1, "The live session did not subscribe to every selected child.");
        first.Results.OnNext("first-result");
        Require(delivered.SequenceEqual(["first-result"]), "The live session lost its child result.");

        var expected = new InvalidOperationException("service stream failed");
        second.Results.OnError(expected);
        Require(ReferenceEquals(failure, expected), "A child failure was not delivered to the application boundary.");
        Require(first.Results.Subscribers == 0, "A sibling child remained subscribed after the terminal failure.");
        var afterFailure = new WorkRow(3, 3, 3);
        source.AddOrUpdate(afterFailure);
        first.Results.OnNext("late-result");
        Require(delivered.SequenceEqual(["first-result"]), "A failed session accepted a late result.");
        Require(afterFailure.Results.Subscribers == 0, "A terminal session subscribed to a row added after its failure.");

        // Recovery is application-owned: the failed session is closed and a
        // fresh selection is opened. DynamicData does not promise an implicit
        // retry or preservation of state from a terminal error.
        using var recoverySource = new SourceCache<WorkRow, int>(row => row.Id);
        var recovered = new WorkRow(4, 4, 4);
        var recoveredValues = new List<string>();
        using var recovery = recoverySource.Connect().MergeMany(row => row.Results).Subscribe(recoveredValues.Add, Fail);
        recoverySource.AddOrUpdate(recovered);
        recovered.Results.OnNext("recovered-result");
        Require(recoveredValues.SequenceEqual(["recovered-result"]), "The replacement session did not recover with its new feed.");
    }

    private static void ViewCloseOwnership()
    {
        using var source = new SourceCache<WorkRow, int>(row => row.Id);
        var first = new WorkRow(1, 10, 10);
        var second = new WorkRow(2, 20, 20);
        var third = new WorkRow(3, 30, 30);
        var replacement = new WorkRow(1, 5, 50);
        source.AddOrUpdate([first, second, third]);
        using var selected = new CurrentSignal<IObservable<IChangeSet<WorkRow, int>>>(source.Connect());
        using var requests = new CurrentSignal<IVirtualRequest>(new VirtualRequest(0, 1));
        var presentations = new List<PresentationRow>();
        var owned = selected.Switch()
            .AutoRefreshOnObservable(row => row.Invalidated)
            .Transform(row =>
            {
                var presentation = new PresentationRow(row.Id, row.Rank);
                presentations.Add(presentation);
                return presentation;
            })
            .DisposeMany()
            .Publish();
        ReadOnlyObservableCollection<PresentationRow>? visible = null;
        IDisposable? viewport = null;
        IDisposable? connection = null;
        try
        {
            viewport = owned.SortAndVirtualize(PresentationRow.ByRank, requests)
                .Bind(out var boundVisible)
                .Subscribe(_ => { }, Fail);
            visible = boundVisible;
            connection = owned.Connect();
            Require(visible.Select(row => row.Id).SequenceEqual([1]), "The first viewport did not bind the current presentation row.");

            source.AddOrUpdate(replacement);
            Require(presentations.Single(row => row.Id == 1 && row.Rank == 10).DisposeCount == 1, "Replacing an owned presentation did not release its previous object.");
            source.RemoveKey(2);
            Require(presentations.Single(row => row.Id == 2).DisposeCount == 1, "Removing an owned presentation did not release it.");
        }
        finally
        {
            // A presentation owns both its subscription and the shared
            // connection. Closing it must release off-screen rows as well.
            if (connection is not null)
            {
                connection.Dispose();
            }

            if (viewport is not null)
            {
                viewport.Dispose();
            }
        }

        Require(presentations.All(row => row.DisposeCount == 1), "Closing the view did not dispose every remaining transformed row exactly once.");
        Require(selected.Subscribers == 0 && requests.Subscribers == 0, "Closing the view retained a feed or viewport subscription.");
        Require(first.Invalidated.Subscribers == 0 && second.Invalidated.Subscribers == 0 && third.Invalidated.Subscribers == 0 && replacement.Invalidated.Subscribers == 0, "Closing the view retained a row invalidation subscription.");
        Require(visible is not null, "The view did not create its bound collection.");
        var closedVisible = visible!;
        var snapshot = closedVisible.Select(row => row.Id).ToArray();
        source.AddOrUpdate(new WorkRow(4, 0, 0));
        requests.OnNext(new VirtualRequest(0, 3));
        Require(closedVisible.Select(row => row.Id).SequenceEqual(snapshot), "A closed view accepted a later source or viewport update.");
    }

    private static void AssertPresentation(IEnumerable<WorkRow> visible, VirtualContext<WorkRow>? context, IEnumerable<int> expected, int total, string stage)
    {
        var expectedRows = expected.ToArray();
        Require(visible.Select(row => row.Id).SequenceEqual(expectedRows), $"{stage}: expected rows [{string.Join(", ", expectedRows)}], got [{string.Join(", ", visible.Select(row => row.Id))}].");
        Require(context?.Response.TotalSize == total && context.Response.StartIndex == 0 && context.Response.Size == expectedRows.Length,
            $"{stage}: expected start 0, size {expectedRows.Length}, total {total}; got {context?.Response.StartIndex}, {context?.Response.Size}, {context?.Response.TotalSize}.");
    }

    private static void AssertAggregates(int? sum, double? average, int? minimum, int? maximum, int expectedSum, double expectedAverage, int expectedMinimum, int expectedMaximum, string stage)
    {
        Require(sum == expectedSum, $"{stage}: expected sum {expectedSum}, got {sum}.");
        Require(average is not null && Math.Abs(average.Value - expectedAverage) < 0.000001, $"{stage}: expected average {expectedAverage}, got {average}.");
        Require(minimum == expectedMinimum && maximum == expectedMaximum, $"{stage}: expected range {expectedMinimum}..{expectedMaximum}, got {minimum}..{maximum}.");
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new InvalidOperationException(message);
        }
    }

    private static void Fail(Exception error) => throw new InvalidOperationException("Unexpected observable failure.", error);
}

internal sealed class WorkRow(int id, int rank, int amount)
{
    internal static IComparer<WorkRow> ByRank { get; } = Comparer<WorkRow>.Create((left, right) => left.Rank.CompareTo(right.Rank));

    internal int Id { get; } = id;

    internal int Rank { get; set; } = rank;

    internal int Amount { get; set; } = amount;

    internal Signal<int> Invalidated { get; } = new();

    internal Signal<string> Results { get; } = new();
}

internal sealed class PresentationRow(int id, int rank) : IDisposable
{
    internal static IComparer<PresentationRow> ByRank { get; } = Comparer<PresentationRow>.Create((left, right) => left.Rank.CompareTo(right.Rank));

    internal int Id { get; } = id;

    internal int Rank { get; } = rank;

    internal int DisposeCount { get; private set; }

    public void Dispose() => DisposeCount++;
}

/// <summary>Small synchronous application signal with inspectable ownership.</summary>
internal class Signal<T> : IObservable<T>, IDisposable
{
    private readonly List<Registration> _registrations = [];
    private Exception? _error;
    private bool _completed;

    internal int Subscribers => _registrations.Count;

    public virtual IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);
        if (_error is not null)
        {
            observer.OnError(_error);
            return Disposable.Empty;
        }

        if (_completed)
        {
            observer.OnCompleted();
            return Disposable.Empty;
        }

        var registration = new Registration(_registrations, observer);
        _registrations.Add(registration);
        return registration;
    }

    internal virtual void OnNext(T value)
    {
        foreach (var registration in _registrations.ToArray())
        {
            registration.Observer.OnNext(value);
        }
    }

    internal void OnError(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        if (_error is not null || _completed)
        {
            return;
        }

        _error = error;
        var registrations = _registrations.ToArray();
        _registrations.Clear();
        foreach (var registration in registrations)
        {
            registration.Observer.OnError(error);
        }
    }

    public void Dispose()
    {
        _completed = true;
        _registrations.Clear();
    }

    private sealed class Registration(List<Registration> registrations, IObserver<T> observer) : IDisposable
    {
        private List<Registration>? _registrations = registrations;

        internal IObserver<T> Observer { get; } = observer;

        public void Dispose()
        {
            var registrations = Interlocked.Exchange(ref _registrations, null);
            registrations?.Remove(this);
        }
    }
}

internal sealed class CurrentSignal<T>(T initial) : Signal<T>
{
    private T _current = initial;

    public override IDisposable Subscribe(IObserver<T> observer)
    {
        var subscription = base.Subscribe(observer);
        observer.OnNext(_current);
        return subscription;
    }

    internal override void OnNext(T value)
    {
        _current = value;
        base.OnNext(value);
    }
}

internal static class Disposable
{
    internal static readonly IDisposable Empty = new EmptyDisposable();

    private sealed class EmptyDisposable : IDisposable
    {
        public void Dispose() { }
    }
}
