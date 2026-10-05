using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Bogus;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif

namespace DynamicData.Tests.Binding;

public static partial class WhenPropertyChangedFixture
{
    public partial class IntegrationTests
    {
        [Test]
        public async Task Shallow_ConcurrentMutationDuringInitialEmit_NotDropped()
        {
            var item = new Item()
            {
                Id = 1,
                Value = 10
            };

            var whenSubscribing = new ManualResetEventSlim();
            var whenValueChanged = new ManualResetEventSlim();

            var source = item.WhenPropertyChanged(
                propertyAccessor:       static item => item.Value,
                notifyOnInitialValue:   true);

            var observedValues = new List<int>();
            var observer = Observer.Create<PropertyValue<Item, int>>(propertyValue =>
            {
                observedValues.Add(propertyValue.Value);

                whenSubscribing.Set();
                whenValueChanged.Wait();
            });

            await Task.WhenAll(
                Task.Run(() =>
                {
                    using var subscription = source.Subscribe(observer);
                }),
                Task.Run(() =>
                {
                    whenSubscribing.Wait();

                    item.Value = 20;

                    whenValueChanged.Set();
                }));

            await Assert.That(observedValues).IsEquivalentTo(new[] { 10, 20 }, TUnit.Assertions.Enums.CollectionOrdering.Matching);
        }
    }
}
