// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;

using Bogus;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Utilities;


namespace DynamicData.Tests.Binding;

/// <summary>
/// Single-threaded contract tests for <see cref="NotifyPropertyChangedEx.WhenPropertyChanged{TObject, TProperty}"/>:
/// handler attachment ordering, subscription cleanup, expression conversions, no-dedup semantics, and deep-chain re-walks on swaps.
/// </summary>
public sealed partial class WhenPropertyChangedBehaviorFixture
{
    /// <summary>An arbitrary observed value; these tests assert handler lifetime, not the value itself.</summary>
    private const double ObservedAmount = 41.375;

    private readonly Randomizer _randomizer;

    /// <summary>Initializes deterministic inputs for property-observation contracts.</summary>
    /// <param name="output">Receives the seed used to generate test inputs.</param>
    public WhenPropertyChangedBehaviorFixture()
    {
        const int seed = 0x35C1_709B;
        _randomizer = new Randomizer(seed);
        Console.WriteLine($"{nameof(WhenPropertyChangedBehaviorFixture)} seed: 0x{seed:X8}");
    }

    [Test]
    public async Task Shallow_NotifyInitialFalse_SubscribesHandlerBeforeReturning()
    {
        // notifyOnInitialValue=false: Subscribe must return only after the PropertyChanged handler
        // is attached. A setter that fires immediately after Subscribe returns must reach the
        // observer.
        var model = new TestModel { Value = 10 };
        var emissions = new List<int>();

        using var sub = model.WhenPropertyChanged(m => m.Value, notifyOnInitialValue: false)
            .Subscribe(pv => emissions.Add(pv.Value));

        model.Value = 20;

        await Assert.That(emissions).IsEquivalentTo(new[] { 20 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task Shallow_NotifyInitialTrue_DoesNotDedupSameValuedEvents()
    {
        var model = new TestModel { Value = 10 };
        var emissions = new List<int>();

        using var sub = model.WhenPropertyChanged(m => m.Value, notifyOnInitialValue: true)
            .Subscribe(pv => emissions.Add(pv.Value));

        model.Value = 10;
        model.Value = 10;
        model.Value = 10;

        await Assert.That(emissions).IsEquivalentTo(new[] { 10, 10, 10, 10 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task Shallow_NotifyInitialFalse_DoesNotDedupSameValuedEvents()
    {
        var model = new TestModel { Value = 10 };
        var emissions = new List<int>();

        using var sub = model.WhenPropertyChanged(m => m.Value, notifyOnInitialValue: false)
            .Subscribe(pv => emissions.Add(pv.Value));

        model.Value = 42;
        model.Value = 42;

        await Assert.That(emissions).IsEquivalentTo(new[] { 42, 42 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DeepChain_NotifyInitialTrue_DoesNotDedupSameValuedEvents()
    {
        var parent = new ParentModel { Child = new ChildModel { Age = 1 } };
        var emissions = new List<int>();

        using var sub = parent.WhenPropertyChanged(p => p.Child!.Age, notifyOnInitialValue: true)
            .Subscribe(pv => emissions.Add(pv.Value));

        parent.Child!.Age = 1;
        parent.Child!.Age = 1;
        parent.Child!.Age = 1;

        await Assert.That(emissions).IsEquivalentTo(new[] { 1, 1, 1, 1 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DeepChain_NotifyInitialFalse_DoesNotDedupSameValuedEvents()
    {
        var parent = new ParentModel { Child = new ChildModel { Age = 1 } };
        var emissions = new List<int>();

        using var sub = parent.WhenPropertyChanged(p => p.Child!.Age, notifyOnInitialValue: false)
            .Subscribe(pv => emissions.Add(pv.Value));

        parent.Child!.Age = 7;
        parent.Child!.Age = 7;

        await Assert.That(emissions).IsEquivalentTo(new[] { 7, 7 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DeepChain_PostSwap_LeafEventOnNewChild_Captured()
    {
        // After parent.Child is reassigned, the leaf-level subscription must be re-attached
        // against the new child. A subsequent leaf mutation on the new child must be captured.
        var parent = new ParentModel { Child = new ChildModel { Age = 10 } };
        var emissions = new List<int>();

        using var sub = parent.WhenPropertyChanged(p => p.Child!.Age, notifyOnInitialValue: true)
            .Subscribe(pv => emissions.Add(pv.Value));

        var newChild = new ChildModel { Age = 20 };
        parent.Child = newChild;
        newChild.Age = 30;

        await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20, 30 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    [Test]
    public async Task DeepChain_MidChainSwap_DeeperLevelsRetargetCorrectly()
    {
        // Mid-chain swap on a 4-level chain. When level 3 is reassigned, the leaf subscription
        // must re-attach against the new subtree; events on the old subtree must be ignored
        // (its notifier subscription was disposed).
        var l1 = new Level1
        {
            Child = new Level2
            {
                Child = new Level3
                {
                    Child = new Level4 { Leaf = 10 },
                },
            },
        };

        var emissions = new List<int>();
        using var sub = l1.WhenPropertyChanged(x => x.Child!.Child!.Child!.Leaf, notifyOnInitialValue: true)
            .Subscribe(pv => emissions.Add(pv.Value));

        await Assert.That(emissions).IsEquivalentTo(new[] { 10 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        var originalLeaf = l1.Child!.Child!.Child!;

        var newL4 = new Level4 { Leaf = 20 };
        l1.Child!.Child!.Child = newL4;

        await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        newL4.Leaf = 30;
        await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20, 30 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);

        originalLeaf.Leaf = 999;
        await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20, 30 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    // https://github.com/reactivemarbles/DynamicData/issues/1149
    [Test]
    public async Task ExpressionContainsImplicitInterfaceCast()
    {
        var child = new ChildModel()
        {
            Age = 10
        };

        using var subscription = ObserveAge(child)
            .RecordValues(out var results);

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues.Count).IsEqualTo(1);
        await Assert.That(results.RecordedValues[0]).IsEqualTo(child.Age);

        ++child.Age;

        await Assert.That(results.Error).IsNull();
        await Assert.That(results.RecordedValues.Skip(1).Count()).IsEqualTo(1);
        await Assert.That(results.RecordedValues[1]).IsEqualTo(child.Age);

        static IObservable<int> ObserveAge<T>(T source)
                where T : IHasAge
            => source.WhenValueChanged(source => source.Age);
    }

    /// <summary>Verifies that a throwing initial observer leaves no property-change handler attached.</summary>
    [Test]
    public async Task Shallow_InitialObserverThrows_DetachesHandler()
    {
        // Arrange
        var model = new ObservablePrice { Amount = ObservedAmount };
        var error = new InvalidOperationException();
        var results = new ValueRecordingObserver<double>(Scheduler.Immediate);
        IObserver<double> observer = results;
        var source = model.WhenValueChanged(static price => price.Amount);

        // Act
        Action subscribe = () =>
        {
            using var subscription = source.Subscribe(value =>
            {
                observer.OnNext(value);
                throw error;
            }, observer.OnError);
        };

        var thrown = await Assert.That(subscribe).Throws<InvalidOperationException>();
        await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(error).Because("the original observer failure must be preserved");
        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the failure occurs during initial delivery");
        await Assert.That(results.Error).IsNull().Because("an observer failure must not be converted into an OnError notification");
        await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
        await Assert.That(model.HandlerCount).IsEqualTo(0).Because("a throwing Subscribe cannot return a disposable to its caller");
    }

    /// <summary>Verifies that a throwing initial observer releases property-change handlers at every chain level.</summary>
    [Test]
    public async Task DeepChain_InitialObserverThrows_DetachesEveryHandler()
    {
        // Arrange
        var leaf = new ObservablePrice { Amount = ObservedAmount };
        var child = new ObservablePrice { Child = leaf };
        var root = new ObservablePrice { Child = child };
        var models = new[] { root, child, leaf };
        var error = new InvalidOperationException();
        var results = new ValueRecordingObserver<double>(Scheduler.Immediate);
        IObserver<double> observer = results;
        var source = root.WhenValueChanged(static price => price.Child!.Child!.Amount);

        // Act
        Action subscribe = () =>
        {
            using var subscription = source.Subscribe(value =>
            {
                observer.OnNext(value);
                throw error;
            }, observer.OnError);
        };

        var thrown = await Assert.That(subscribe).Throws<InvalidOperationException>();
        await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(error).Because("the original observer failure must be preserved");
        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the failure occurs during initial delivery");
        await Assert.That(results.Error).IsNull().Because("an observer failure must not be converted into an OnError notification");
        await Assert.That(models.All(model => model.WasSubscribed)).IsTrue().Because("each observable level must be registered before it is read");
        await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("failed initialization must release every handler, not just the root handler");
    }

    /// <summary>Verifies that an initial getter failure releases the handler when the default error handler throws.</summary>
    [Test]
    public async Task Shallow_InitialGetterThrows_DefaultErrorHandler_DetachesHandler()
    {
        // Arrange
        var error = new InvalidOperationException();
        var model = new ObservablePrice { Amount = ObservedAmount, ReadError = error };
        var source = model.WhenValueChanged(static price => price.Amount);

        // Act
        Action subscribe = () =>
        {
            using var subscription = source.Subscribe();
        };

        var thrown = await Assert.That(subscribe).Throws<InvalidOperationException>();
        await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(error).Because("the original getter failure must be preserved");
        await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
        await Assert.That(model.HandlerCount).IsEqualTo(0).Because("failed initialization must not retain the event handler");
    }

    /// <summary>Verifies that a failing chain getter releases every handler when the default error handler throws.</summary>
    /// <param name="notifyOnInitialValue">Whether subscribing requests an initial value notification.</param>
    /// <param name="failBeforeLeaf">Whether an intermediate getter fails before the leaf can be subscribed.</param>
    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, true)]
    public async Task DeepChain_InitialGetterThrows_DefaultErrorHandler_DetachesEveryHandler(bool notifyOnInitialValue, bool failBeforeLeaf)
    {
        // Arrange
        var error = new InvalidOperationException();
        var leaf = new ObservablePrice { Amount = ObservedAmount, ReadError = failBeforeLeaf ? null : error };
        var child = new ObservablePrice { Child = leaf, ChildReadError = failBeforeLeaf ? error : null };
        var root = new ObservablePrice { Child = child };
        var models = new[] { root, child, leaf };
        var source = root.WhenValueChanged(static price => price.Child!.Child!.Amount, notifyOnInitialValue);

        // Act
        Action subscribe = () =>
        {
            using var subscription = source.Subscribe();
        };

        var thrown = await Assert.That(subscribe).Throws<Exception>();
        await Assert.That(thrown.GetBaseException()).IsSameReferenceAs(error).Because("the failure must originate in the observed getter");
        await Assert.That(root.WasSubscribed).IsTrue().Because("the root handler must attach before its child is read");
        await Assert.That(child.WasSubscribed).IsTrue().Because("the intermediate handler must attach before its child is read");
        await Assert.That(leaf.WasSubscribed).IsEqualTo(!failBeforeLeaf).Because("the leaf is reachable only if the intermediate getter succeeds");
        await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("failed initialization must release handlers at every visited level");
    }

    /// <summary>Verifies that a handled initial getter failure terminates observation and releases its event handler.</summary>
    [Test]
    public async Task Shallow_InitialGetterThrows_ErrorIsRecordedAndHandlerDetached()
    {
        // Arrange
        var error = new InvalidOperationException();
        var model = new ObservablePrice { Amount = ObservedAmount, ReadError = error };

        // Act
        using var subscription = model.WhenPropertyChanged(static price => price.Amount)
            .RecordValues(out var results);

        await Assert.That(results.Error).IsSameReferenceAs(error).Because("getter failures must be delivered through OnError");
        await Assert.That(results.RecordedValues).IsEmpty().Because("the initial getter did not produce a value");
        await Assert.That(results.HasCompleted).IsFalse().Because("OnError is the terminal notification");
        await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
        await Assert.That(model.HandlerCount).IsEqualTo(0).Because("OnError must release the handler before Subscribe returns");
    }

    /// <summary>Verifies that a handled chain getter failure terminates observation and releases every event handler.</summary>
    /// <param name="notifyOnInitialValue">Whether subscribing requests an initial value notification.</param>
    /// <param name="failBeforeLeaf">Whether an intermediate getter fails before the leaf can be subscribed.</param>
    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, true)]
    public async Task DeepChain_InitialGetterThrows_ErrorIsRecordedAndEveryHandlerDetached(bool notifyOnInitialValue, bool failBeforeLeaf)
    {
        // Arrange
        var error = new InvalidOperationException();
        var leaf = new ObservablePrice { Amount = ObservedAmount, ReadError = failBeforeLeaf ? null : error };
        var child = new ObservablePrice { Child = leaf, ChildReadError = failBeforeLeaf ? error : null };
        var root = new ObservablePrice { Child = child };
        var models = new[] { root, child, leaf };

        // Act
        using var subscription = root.WhenPropertyChanged(static price => price.Child!.Child!.Amount, notifyOnInitialValue)
            .RecordValues(out var results);

        await Assert.That(results.Error).IsNotNull().Because("chain getter failures must be delivered through OnError");
        await Assert.That(results.Error!.GetBaseException()).IsSameReferenceAs(error).Because("the failure must originate in the observed getter");
        await Assert.That(results.RecordedValues).IsEmpty().Because("the chain did not produce an obtainable value");
        await Assert.That(results.HasCompleted).IsFalse().Because("OnError is the terminal notification");
        await Assert.That(root.WasSubscribed).IsTrue().Because("the root handler must attach before its child is read");
        await Assert.That(child.WasSubscribed).IsTrue().Because("the intermediate handler must attach before its child is read");
        await Assert.That(leaf.WasSubscribed).IsEqualTo(!failBeforeLeaf).Because("the leaf is reachable only if the intermediate getter succeeds");
        await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("OnError must release every handler before Subscribe returns");
    }

    /// <summary>Verifies that live property handlers belong to the returned subscription until it is disposed.</summary>
    /// <param name="deepChain">Whether the observed property is reached through intermediate objects.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Subscription_ExplicitDisposal_ReleasesHandlers(bool deepChain)
    {
        // Arrange
        var leaf = new ObservablePrice { Amount = ObservedAmount };
        var child = new ObservablePrice { Child = leaf };
        var root = new ObservablePrice { Child = child };
        var models = deepChain ? new[] { root, child, leaf } : new[] { leaf };
        var source = deepChain
            ? root.WhenValueChanged(static price => price.Child!.Child!.Amount)
            : leaf.WhenValueChanged(static price => price.Amount);
        using var subscription = source.RecordValues(out var results);
        var attachedHandlerCounts = models.Select(model => model.HandlerCount).ToArray();

        // Act
        subscription.Dispose();

        await Assert.That(attachedHandlerCounts.All(count => count == 1)).IsTrue().Because("each visited object must stay subscribed after initialization");
        await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("disposing the returned subscription must release every retained handler");
        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("initialization must publish the observed value");
        await Assert.That(results.Error).IsNull().Because("explicit disposal is not an observation failure");
        await Assert.That(results.HasCompleted).IsFalse().Because("unsubscribing does not publish a completion notification");
    }

    /// <summary>Verifies that synchronous completion during initial delivery releases every property handler.</summary>
    /// <param name="deepChain">Whether the observed property is reached through intermediate objects.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Subscription_SynchronousCompletion_ReleasesHandlers(bool deepChain)
    {
        // Arrange
        var leaf = new ObservablePrice { Amount = ObservedAmount };
        var child = new ObservablePrice { Child = leaf };
        var root = new ObservablePrice { Child = child };
        var models = deepChain ? new[] { root, child, leaf } : new[] { leaf };
        var source = deepChain
            ? root.WhenValueChanged(static price => price.Child!.Child!.Amount)
            : leaf.WhenValueChanged(static price => price.Amount);

        // Act
        using var subscription = source.Take(1)
            .RecordValues(out var results);

        await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the requested initial value must be delivered");
        await Assert.That(results.Error).IsNull().Because("taking an initial value is normal completion");
        await Assert.That(results.HasCompleted).IsTrue().Because("Take completes after receiving its requested value");
        await Assert.That(models.All(model => model.WasSubscribed)).IsTrue().Because("handlers must attach before initial delivery");
        await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("synchronous completion must release handlers before Subscribe returns");
    }

    /// <summary>
    /// An observable input with numeric properties, whose custom event accessors expose property
    /// subscription lifetimes and whose getters can be made to fail on demand.
    /// </summary>
    private sealed class ObservablePrice : INotifyPropertyChanged
    {
        private double _amount;
        private ObservablePrice? _child;
        private double _otherAmount;
        private PropertyChangedEventHandler? _propertyChanged;

        /// <inheritdoc />
        public event PropertyChangedEventHandler? PropertyChanged
        {
            add
            {
                WasSubscribed = true;
                _propertyChanged += value;
            }

            remove => _propertyChanged -= value;
        }

        /// <summary>Gets or sets the amount and raises a property-change notification when set.</summary>
        public double Amount
        {
            get => ReadError is null ? _amount : throw ReadError;
            set
            {
                _amount = value;
                _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Amount)));
            }
        }

        /// <summary>Gets the next object in a nested property path.</summary>
        public ObservablePrice? Child
        {
            get => ChildReadError is null ? _child : throw ChildReadError;
            init => _child = value;
        }

        /// <summary>Gets an optional failure raised when reading <see cref="Child"/>.</summary>
        public InvalidOperationException? ChildReadError { get; init; }

        /// <summary>Gets the number of event handlers retained by this object.</summary>
        public int HandlerCount => _propertyChanged?.GetInvocationList().Length ?? 0;

        /// <summary>Gets an optional failure raised when reading <see cref="Amount"/>.</summary>
        public InvalidOperationException? ReadError { get; init; }

        /// <summary>Gets or sets a second amount, used to observe two converted paths on one object.</summary>
        public double OtherAmount
        {
            get => _otherAmount;
            set
            {
                _otherAmount = value;
                _propertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(OtherAmount)));
            }
        }

        /// <summary>Gets whether any observer has registered a property-change handler.</summary>
        public bool WasSubscribed { get; private set; }
    }

    private interface IHasAge
        : INotifyPropertyChanged
    {
        int Age { get; }
    }

    private sealed class TestModel : INotifyPropertyChanged
    {
        private int _value;

        public event PropertyChangedEventHandler? PropertyChanged;

        public int Value
        {
            get => _value;
            set
            {
                _value = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Value)));
            }
        }
    }

    private sealed class ParentModel : INotifyPropertyChanged
    {
        private ChildModel? _child;

        public event PropertyChangedEventHandler? PropertyChanged;

        public ChildModel? Child
        {
            get => _child;
            set
            {
                _child = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }
    }

    private sealed class ChildModel
        : IHasAge
    {
        private int _age;

        public event PropertyChangedEventHandler? PropertyChanged;

        public int Age
        {
            get => _age;
            set
            {
                _age = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Age)));
            }
        }
    }

    private sealed class Level1 : INotifyPropertyChanged
    {
        private Level2? _child;

        public event PropertyChangedEventHandler? PropertyChanged;

        public Level2? Child
        {
            get => _child;
            set
            {
                _child = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }
    }

    private sealed class Level2 : INotifyPropertyChanged
    {
        private Level3? _child;

        public event PropertyChangedEventHandler? PropertyChanged;

        public Level3? Child
        {
            get => _child;
            set
            {
                _child = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }
    }

    private sealed class Level3 : INotifyPropertyChanged
    {
        private Level4? _child;

        public event PropertyChangedEventHandler? PropertyChanged;

        public Level4? Child
        {
            get => _child;
            set
            {
                _child = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Child)));
            }
        }
    }

    private sealed class Level4 : INotifyPropertyChanged
    {
        private int _leaf;

        public event PropertyChangedEventHandler? PropertyChanged;

        public int Leaf
        {
            get => _leaf;
            set
            {
                _leaf = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(Leaf)));
            }
        }
    }
}
