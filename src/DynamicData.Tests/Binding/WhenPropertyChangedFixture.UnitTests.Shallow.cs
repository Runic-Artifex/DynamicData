#if REACTIVE_TESTS
using ImmediateTestScheduler = System.Reactive.Concurrency.ImmediateScheduler;
#else
using ImmediateTestScheduler = ReactiveUI.Primitives.Concurrency.ImmediateSequencer;
#endif
using System;
using System.Collections.Generic;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.Binding;

public static partial class WhenPropertyChangedFixture
{
    public partial class UnitTests
    {
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

            // Assert
            var thrown3 = await Assert.That(subscribe).Throws<InvalidOperationException>().Because("the default Rx error handler must rethrow the getter failure");
            await Assert.That(thrown3).IsSameReferenceAs(error).Because("the original getter failure must be preserved");
            await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
            await Assert.That(model.HandlerCount).IsEqualTo(0).Because("failed initialization must not retain the event handler");
        }

        [Test]
        public async Task Shallow_InitialGetterThrows_ErrorIsRecordedAndHandlerDetached()
        {
            // Arrange
            var error = new InvalidOperationException();
            var model = new ObservablePrice { Amount = ObservedAmount, ReadError = error };

            // Act
            using var subscription = model.WhenPropertyChanged(static price => price.Amount)
                .RecordValues(out var results);

            // Assert
            await Assert.That(results.Error).IsSameReferenceAs(error).Because("getter failures must be delivered through OnError");
            await Assert.That(results.RecordedValues).IsEmpty().Because("the initial getter did not produce a value");
            await Assert.That(results.HasCompleted).IsFalse().Because("OnError is the terminal notification");
            await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
            await Assert.That(model.HandlerCount).IsEqualTo(0).Because("OnError must release the handler before Subscribe returns");
        }

        [Test]
        public async Task Shallow_InitialObserverThrows_DetachesHandler()
        {
            // Arrange
            var model = new ObservablePrice { Amount = ObservedAmount };
            var error = new InvalidOperationException();
            var results = new ValueRecordingObserver<double>(ImmediateTestScheduler.Instance);
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

            // Assert
            var thrown4 = await Assert.That(subscribe).Throws<InvalidOperationException>().Because("observer failures must escape Subscribe");
            await Assert.That(thrown4).IsSameReferenceAs(error).Because("the original observer failure must be preserved");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the failure occurs during initial delivery");
            await Assert.That(results.Error).IsNull().Because("an observer failure must not be converted into an OnError notification");
            await Assert.That(model.WasSubscribed).IsTrue().Because("registration must precede the initial value read");
            await Assert.That(model.HandlerCount).IsEqualTo(0).Because("a throwing Subscribe cannot return a disposable to its caller");
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
    }
}
