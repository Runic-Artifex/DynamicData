using System;
using System.Linq;

using Bogus;

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
        private readonly Randomizer _randomizer
            = new(0x35C1_709B);

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

            await Assert.That(results.Error).IsNull().Because("no errors should have occurred");
            await Assert.That(results.RecordedValues.Count).IsEqualTo(1).Because("the initial value of the observed expression should have been published");
            await Assert.That(results.RecordedValues[0]).IsEqualTo(child.Age).Because("the initial value of the observed expression should have been published");

            ++child.Age;

            await Assert.That(results.Error).IsNull().Because("no errors should have occurred");
            await Assert.That(results.RecordedValues.Skip(1).Count()).IsEqualTo(1).Because("the value of the observed expression changed once");
            await Assert.That(results.RecordedValues[1]).IsEqualTo(child.Age).Because("the correct value should have been published");

            static IObservable<int> ObserveAge<T>(T source)
                where T : IHasAge
                => source.WhenValueChanged(source => source.Age);
        }
    }
}
