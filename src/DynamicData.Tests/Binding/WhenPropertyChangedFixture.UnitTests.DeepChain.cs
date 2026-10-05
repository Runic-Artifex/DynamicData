#if REACTIVE_TESTS
using ImmediateTestScheduler = System.Reactive.Concurrency.ImmediateScheduler;
#else
using ImmediateTestScheduler = ReactiveUI.Primitives.Concurrency.ImmediateSequencer;
#endif
using System;
using System.Collections.Generic;
using System.Linq;

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

            // Assert
            var thrown1 = await Assert.That(subscribe).Throws<Exception>().Because("the default Rx error handler must rethrow initialization failures");
            await Assert.That(thrown1.GetBaseException()).IsSameReferenceAs(error).Because("the failure must originate in the observed getter");
            await Assert.That(root.WasSubscribed).IsTrue().Because("the root handler must attach before its child is read");
            await Assert.That(child.WasSubscribed).IsTrue().Because("the intermediate handler must attach before its child is read");
            await Assert.That(leaf.WasSubscribed).IsEqualTo(!failBeforeLeaf).Because("the leaf is reachable only if the intermediate getter succeeds");
            await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("failed initialization must release handlers at every visited level");
        }

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

            // Assert
            await Assert.That(results.Error).IsNotNull().Because("chain getter failures must be delivered through OnError");
            await Assert.That(results.Error!.GetBaseException()).IsSameReferenceAs(error).Because("the failure must originate in the observed getter");
            await Assert.That(results.RecordedValues).IsEmpty().Because("the chain did not produce an obtainable value");
            await Assert.That(results.HasCompleted).IsFalse().Because("OnError is the terminal notification");
            await Assert.That(root.WasSubscribed).IsTrue().Because("the root handler must attach before its child is read");
            await Assert.That(child.WasSubscribed).IsTrue().Because("the intermediate handler must attach before its child is read");
            await Assert.That(leaf.WasSubscribed).IsEqualTo(!failBeforeLeaf).Because("the leaf is reachable only if the intermediate getter succeeds");
            await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("OnError must release every handler before Subscribe returns");
        }

        [Test]
        public async Task DeepChain_InitialObserverThrows_DetachesEveryHandler()
        {
            // Arrange
            var leaf = new ObservablePrice { Amount = ObservedAmount };
            var child = new ObservablePrice { Child = leaf };
            var root = new ObservablePrice { Child = child };
            var models = new[] { root, child, leaf };
            var error = new InvalidOperationException();
            var results = new ValueRecordingObserver<double>(ImmediateTestScheduler.Instance);
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

            // Assert
            var thrown2 = await Assert.That(subscribe).Throws<InvalidOperationException>().Because("observer failures must escape Subscribe");
            await Assert.That(thrown2).IsSameReferenceAs(error).Because("the original observer failure must be preserved");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the failure occurs during initial delivery");
            await Assert.That(results.Error).IsNull().Because("an observer failure must not be converted into an OnError notification");
            await Assert.That(models.All(model => model.WasSubscribed)).IsTrue().Because("each observable level must be registered before it is read");
            await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("failed initialization must release every handler, not just the root handler");
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

            await Assert.That(emissions).IsEquivalentTo(new[] { 10 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("initial emission");

            var originalLeaf = l1.Child!.Child!.Child!;

            var newL4 = new Level4 { Leaf = 20 };
            l1.Child!.Child!.Child = newL4;

            await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("mid-chain swap emits the new leaf value");

            newL4.Leaf = 30;
            await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20, 30 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("leaf event on new subtree is captured");

            originalLeaf.Leaf = 999;
            await Assert.That(emissions).IsEquivalentTo(new[] { 10, 20, 30 }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("leaf event on detached subtree is ignored");
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
    }
}
