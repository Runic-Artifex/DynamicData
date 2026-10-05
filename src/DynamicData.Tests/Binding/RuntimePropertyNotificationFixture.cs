// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.Binding;

public sealed class RuntimePropertyNotificationFixture
{
    [Test]
    public async Task InterfaceDeclaredProperty_FollowsRuntimeNotifierAndReplacement()
    {
        var first = new NotifyingValue { Value = 1 };
        var replacement = new NotifyingValue { Value = 3 };
        var source = new Parent<IValue> { Child = first };
        using var subscription = source.WhenValueChanged(static parent => parent.Child!.Value, fallbackValue: static () => -1)
            .RecordValues(out var results);

        first.Value = 2;
        source.Child = replacement;
        first.Value = 100;
        replacement.Value = 4;
        source.Child = null;
        replacement.Value = 100;
        source.Child = first;
        first.Value = 5;

        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 1, 2, 3, 4, -1, 100, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Error).IsNull();
        await Assert.That(replacement.HandlerCount).IsEqualTo(0);
        await Assert.That(first.HandlerCount).IsEqualTo(1);

        subscription.Dispose();
        await Assert.That(first.HandlerCount).IsEqualTo(0);
        await Assert.That(source.HandlerCount).IsEqualTo(0);
    }

    [Test]
    public async Task ObjectDeclaredProperty_CastObservesRuntimeNotifierAndAllowsPlainReplacement()
    {
        var first = new NotifyingValue { Value = 1 };
        var source = new Parent<object> { Child = first };
        using var subscription = source.WhenValueChanged(static parent => ((IValue)parent.Child!).Value)
            .RecordValues(out var results);

        first.Value = 2;
        var plain = new PlainValue { Value = 3 };
        source.Child = plain;
        first.Value = 100;
        plain.Value = 4;
        source.Raise(nameof(source.Child));
        var replacement = new NotifyingValue { Value = 5 };
        source.Child = replacement;
        replacement.Value = 6;

        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 1, 2, 3, 4, 5, 6 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Error).IsNull();
        await Assert.That(first.HandlerCount).IsEqualTo(0);
        await Assert.That(replacement.HandlerCount).IsEqualTo(1);

        subscription.Dispose();
        await Assert.That(replacement.HandlerCount).IsEqualTo(0);
        await Assert.That(source.HandlerCount).IsEqualTo(0);
    }

    [Test]
    public async Task BaseDeclaredProperty_ObservesDerivedRuntimeNotifier()
    {
        var child = new NotifyingValue { Value = 1 };
        var source = new Parent<PlainValue> { Child = child };
        using var subscription = source.WhenValueChanged(static parent => parent.Child!.Value).RecordValues(out var results);

        child.Value = 2;

        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 1, 2 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Error).IsNull();
        await Assert.That(child.HandlerCount).IsEqualTo(1);
    }

    [Test]
    [Arguments(null)]
    [Arguments("")]
    public async Task WildcardNotification_RewalksChainAndUpdatesShallowProperty(string? propertyName)
    {
        var first = new NotifyingValue { Value = 1 };
        var source = new Parent<IValue> { Child = first };
        using var deepSubscription = source.WhenValueChanged(static parent => parent.Child!.Value).RecordValues(out var deep);
        using var shallowSubscription = first.WhenValueChanged(static value => value.Value).RecordValues(out var shallow);

        first.SetSilently(2);
        first.Raise("Unrelated");
        first.Raise(propertyName);
        var replacement = new NotifyingValue { Value = 3 };
        source.SetSilently(replacement);
        source.Raise(propertyName);
        first.Value = 4;
        replacement.SetSilently(5);
        replacement.Raise(propertyName);

        await Assert.That(deep.RecordedValues).IsEquivalentTo(new[] { 1, 2, 3, 5 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(shallow.RecordedValues).IsEquivalentTo(new[] { 1, 2, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(deep.Error).IsNull();
        await Assert.That(shallow.Error).IsNull();
        await Assert.That(first.HandlerCount).IsEqualTo(1).Because("only the separate shallow observation retains the replaced child");
    }

    [Test]
    public async Task UserDefinedConversion_SelectsRuntimeNotificationTarget()
    {
        var first = new NotifyingValue { Value = 1 };
        var source = new Parent<ValueHandle> { Child = new ValueHandle(first) };
        using var subscription = source.WhenValueChanged(static parent => ((PlainValue)parent.Child!).Value).RecordValues(out var results);

        first.Value = 2;
        var replacement = new NotifyingValue { Value = 3 };
        source.Child = new ValueHandle(replacement);
        replacement.Value = 4;

        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { 1, 2, 3, 4 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        await Assert.That(results.Error).IsNull();
        await Assert.That(first.HandlerCount).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RuntimeGetterFailure_RoutesToErrorAndReleasesHandlers(bool duringInitialization)
    {
        var failure = new InvalidOperationException("getter");
        var child = new NotifyingValue { Value = 1, ReadError = duringInitialization ? failure : null };
        var source = new Parent<IValue> { Child = child };
        using var subscription = source.WhenValueChanged(static parent => parent.Child!.Value).RecordValues(out var results);

        if (!duringInitialization)
        {
            child.ReadError = failure;
            child.Raise(nameof(child.Value));
        }

        await Assert.That(results.Error!.GetBaseException()).IsSameReferenceAs(failure);
        await Assert.That(results.RecordedValues.Count).IsEqualTo(duringInitialization ? 0 : 1);
        await Assert.That(results.HasCompleted).IsFalse();
        await Assert.That(source.HandlerCount).IsEqualTo(0);
        await Assert.That(child.HandlerCount).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RuntimeObserverFailure_PropagatesWithoutErrorNotification(bool duringInitialization)
    {
        var failure = new InvalidOperationException("observer");
        var child = new NotifyingValue { Value = 1 };
        var source = new Parent<IValue> { Child = child };
        Exception? reportedError = null;
        var observable = source.WhenValueChanged(static parent => parent.Child!.Value, notifyOnInitialValue: duringInitialization);
        IDisposable? subscription = null;

        Action act = () =>
        {
            subscription = observable.Subscribe(_ => throw failure, error => reportedError = error);
            if (!duringInitialization)
                child.Value = 2;
        };

        try
        {
            var thrown = await Assert.That(act).Throws<InvalidOperationException>();
            await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(failure);
            await Assert.That(reportedError).IsNull().Because("observer exceptions must not be converted into getter errors");
        }
        finally
        {
            subscription?.Dispose();
        }

        await Assert.That(source.HandlerCount).IsEqualTo(0);
        await Assert.That(child.HandlerCount).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RuntimeErrorHandlerFailure_PropagatesAndReleasesHandlers(bool duringInitialization)
    {
        var getterFailure = new InvalidOperationException("getter");
        var handlerFailure = new InvalidOperationException("error handler");
        var child = new NotifyingValue { Value = 1, ReadError = duringInitialization ? getterFailure : null };
        var source = new Parent<IValue> { Child = child };
        Exception? reportedError = null;
        IDisposable? subscription = null;

        Action act = () =>
        {
            subscription = source.WhenValueChanged(static parent => parent.Child!.Value).Subscribe(_ => { }, error =>
            {
                reportedError = error;
                throw handlerFailure;
            });
            if (!duringInitialization)
            {
                child.ReadError = getterFailure;
                child.Raise(nameof(child.Value));
            }
        };

        try
        {
            var thrown = await Assert.That(act).Throws<InvalidOperationException>();
            await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(handlerFailure);
            await Assert.That(reportedError!.GetBaseException()).IsSameReferenceAs(getterFailure);
            await Assert.That(source.HandlerCount).IsEqualTo(0);
            await Assert.That(child.HandlerCount).IsEqualTo(0);
        }
        finally
        {
            subscription?.Dispose();
        }
    }

    private interface IValue
    {
        int Value { get; }
    }

    private class PlainValue : IValue
    {
        private int _value;

        public Exception? ReadError { get; set; }

        public virtual int Value
        {
            get => ReadError is null ? _value : throw ReadError;
            set => _value = value;
        }
    }

    private sealed class NotifyingValue : PlainValue, INotifyPropertyChanged
    {
        private PropertyChangedEventHandler? _handlers;

        public event PropertyChangedEventHandler? PropertyChanged
        {
            add => _handlers += value;
            remove => _handlers -= value;
        }

        public int HandlerCount => _handlers?.GetInvocationList().Length ?? 0;

        public override int Value
        {
            get => base.Value;
            set
            {
                base.Value = value;
                Raise(nameof(Value));
            }
        }

        public void SetSilently(int value) => base.Value = value;

        public void Raise(string? propertyName) => _handlers?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }

    private sealed class Parent<T> : INotifyPropertyChanged
        where T : class
    {
        private PropertyChangedEventHandler? _handlers;
        private T? _child;

        public event PropertyChangedEventHandler? PropertyChanged
        {
            add => _handlers += value;
            remove => _handlers -= value;
        }

        public int HandlerCount => _handlers?.GetInvocationList().Length ?? 0;

        public T? Child
        {
            get => _child;
            set
            {
                _child = value;
                Raise(nameof(Child));
            }
        }

        public void SetSilently(T child) => _child = child;

        public void Raise(string? propertyName) => _handlers?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }

    private sealed class ValueHandle(PlainValue value)
    {
        public static explicit operator PlainValue(ValueHandle handle) => handle.Value;

        private PlainValue Value { get; } = value;
    }
}
