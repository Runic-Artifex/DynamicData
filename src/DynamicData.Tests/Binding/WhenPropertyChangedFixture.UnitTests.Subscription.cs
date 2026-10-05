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

            // Assert
            await Assert.That(attachedHandlerCounts.All(count => count == 1)).IsTrue().Because("each visited object must stay subscribed after initialization");
            await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("disposing the returned subscription must release every retained handler");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("initialization must publish the observed value");
            await Assert.That(results.Error).IsNull().Because("explicit disposal is not an observation failure");
            await Assert.That(results.HasCompleted).IsFalse().Because("unsubscribing does not publish a completion notification");
        }

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

            // Assert
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { ObservedAmount }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the requested initial value must be delivered");
            await Assert.That(results.Error).IsNull().Because("taking an initial value is normal completion");
            await Assert.That(results.HasCompleted).IsTrue().Because("Take completes after receiving its requested value");
            await Assert.That(models.All(model => model.WasSubscribed)).IsTrue().Because("handlers must attach before initial delivery");
            await Assert.That(models.Select(model => model.HandlerCount).All(count => count == 0)).IsTrue().Because("synchronous completion must release handlers before Subscribe returns");
        }
    }
}
