using System;
using System.ComponentModel;
using System.Linq.Expressions;

#if REACTIVE_TESTS
using DynamicData.Reactive.Binding;
#else
using DynamicData.Binding;
#endif
using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Binding;

public static partial class WhenPropertyChangedFixture
{
    public partial class UnitTests
    {
        [Test]
        public async Task NumericConversionBeforePropertyAccess_InitialValue_IsObserved()
        {
            // Arrange
            // An odd numerator produces an exactly representable, non-integral amount.
            var amount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var model = new ObservablePrice { Amount = amount };
            var expectedScale = ((decimal)amount).Scale;

            // Act
            using var subscription = model.WhenValueChanged(static price => ((decimal)price.Amount).Scale)
                .RecordValues(out var results);

            // Assert
            await Assert.That(results.Error).IsNull().Because("the next property belongs to the converted decimal, not the source double");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { expectedScale }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the initial value must match the supplied expression");
            await Assert.That(results.HasCompleted).IsFalse().Because("further amount changes remain observable");
        }

        /// <summary>Verifies that property changes remain observable through a value-changing numeric conversion.</summary>
        /// <param name="notifyOnInitialValue">Whether subscribing requests an initial value notification.</param>
        [Test]
        [Arguments(false)]
        [Arguments(true)]
        public async Task NumericConversionBeforePropertyAccess_PropertyChanges_AreObserved(bool notifyOnInitialValue)
        {
            // Arrange
            // Odd quarters and eighths have distinct decimal scales without floating-point rounding.
            var initialAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var changedAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 8d;
            var model = new ObservablePrice { Amount = initialAmount };
            var initialScale = ((decimal)initialAmount).Scale;
            var changedScale = ((decimal)changedAmount).Scale;
            var expectedScales = notifyOnInitialValue ? new[] { initialScale, changedScale } : new[] { changedScale };
            using var subscription = model.WhenValueChanged(static price => ((decimal)price.Amount).Scale, notifyOnInitialValue)
                .RecordValues(out var results);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(results.Error).IsNull().Because("conversion must be applied on both initial and subsequent chain reads");
            await Assert.That(results.RecordedValues).IsEquivalentTo(expectedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("notifications must match the expression and initial-value option");
            await Assert.That(results.HasCompleted).IsFalse().Because("amount changes do not complete the observation");
        }

        /// <summary>Verifies that a numeric conversion at the end of a property path preserves initial and changed values.</summary>
        [Test]
        public async Task NumericConversionAtLeaf_PropertyChanges_AreObserved()
        {
            // Arrange
            var initialAmount = _randomizer.Double();
            var changedAmount = initialAmount + _randomizer.Double(1, 2);
            var model = new ObservablePrice { Amount = initialAmount };
            var expectedAmounts = new[] { (decimal)initialAmount, (decimal)changedAmount };
            using var subscription = model.WhenValueChanged(static price => (decimal)price.Amount)
                .RecordValues(out var results);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(results.Error).IsNull().Because("a conversion must also remain supported as the final expression step");
            await Assert.That(results.RecordedValues).IsEquivalentTo(expectedAmounts, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("each observed value must be converted to the requested type");
            await Assert.That(results.HasCompleted).IsFalse().Because("further amount changes remain observable");
        }

        /// <summary>Verifies that a reference cast preserves observation of properties on the runtime model.</summary>
        [Test]
        public async Task ReferenceCastBeforePropertyAccess_PropertyChanges_AreObserved()
        {
            // Arrange
            var person = Fakers.Person.Clone().WithSeed(_randomizer).Generate();
            INotifyPropertyChanged model = person;
            var initialAge = person.Age;
            var changedAge = initialAge + _randomizer.Int(1, byte.MaxValue);
            using var subscription = model.WhenValueChanged(static source => ((Person)source).Age)
                .RecordValues(out var results);

            // Act
            person.Age = changedAge;

            // Assert
            await Assert.That(results.Error).IsNull().Because("supported reference casts must preserve property observation");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { initialAge, changedAge }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the runtime model remains the notification source");
            await Assert.That(results.HasCompleted).IsFalse().Because("further age changes remain observable");
        }

        /// <summary>Verifies that an interface cast preserves observation after replacing an intermediate object.</summary>
        [Test]
        public async Task InterfaceCastBeforePropertyAccess_ReplacementChildChanges_AreObserved()
        {
            // Arrange
            var initialAge = _randomizer.Int(1, byte.MaxValue);
            var replacementAge = initialAge + _randomizer.Int(1, byte.MaxValue);
            var changedAge = replacementAge + _randomizer.Int(1, byte.MaxValue);
            var parent = new ParentModel { Child = new ChildModel { Age = initialAge } };
            var replacement = new ChildModel { Age = replacementAge };
            using var subscription = parent.WhenValueChanged(static source => ((IHasAge)source.Child!).Age)
                .RecordValues(out var results);
            parent.Child = replacement;

            // Act
            replacement.Age = changedAge;

            // Assert
            await Assert.That(results.Error).IsNull().Because("supported interface casts must preserve nested property observation");
            await Assert.That(results.RecordedValues).IsEquivalentTo(new[] { initialAge, replacementAge, changedAge }, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the interface property must follow the replacement child and its subsequent changes");
            await Assert.That(results.HasCompleted).IsFalse().Because("further child changes remain observable");
        }

        /// <summary>Verifies that converted properties retain independent values and notification sources.</summary>
        /// <param name="changeAmount">Whether to change the first observed property rather than the second.</param>
        [Test]
        [Arguments(false)]
        [Arguments(true)]
        public async Task NumericConversionsOnDifferentProperties_Changes_AreIndependent(bool changeAmount)
        {
            // Arrange
            var initialAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var initialOtherAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 8d;
            var changedAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 16d;
            var model = new ObservablePrice { Amount = initialAmount, OtherAmount = initialOtherAmount };
            var initialScale = ((decimal)initialAmount).Scale;
            var initialOtherScale = ((decimal)initialOtherAmount).Scale;
            var changedScale = ((decimal)changedAmount).Scale;
            var expectedScales = changeAmount ? new[] { initialScale, changedScale } : new[] { initialScale };
            var expectedOtherScales = changeAmount ? new[] { initialOtherScale } : new[] { initialOtherScale, changedScale };
            using var amountSubscription = model.WhenValueChanged(static price => ((decimal)price.Amount).Scale)
                .RecordValues(out var amounts);
            using var otherSubscription = model.WhenValueChanged(static price => ((decimal)price.OtherAmount).Scale)
                .RecordValues(out var otherAmounts);

            // Act
            if (changeAmount)
            {
                model.Amount = changedAmount;
            }
            else
            {
                model.OtherAmount = changedAmount;
            }

            // Assert
            await Assert.That(amounts.Error).IsNull().Because("the first converted property remains observable");
            await Assert.That(otherAmounts.Error).IsNull().Because("the second converted property remains observable");
            await Assert.That(amounts.RecordedValues).IsEquivalentTo(expectedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the first path observes only its own property");
            await Assert.That(otherAmounts.RecordedValues).IsEquivalentTo(expectedOtherScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the second path observes only its own property");
            await Assert.That(amounts.HasCompleted).IsFalse().Because("the first observation remains active");
            await Assert.That(otherAmounts.HasCompleted).IsFalse().Because("the second observation remains active");
        }

        /// <summary>Verifies that different conversions of one property retain their own evaluation paths.</summary>
        [Test]
        public async Task DifferentConversionPathsOnSameProperty_Changes_UseEachConversion()
        {
            // Arrange
            var initialAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var changedAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 8d;
            var model = new ObservablePrice { Amount = initialAmount };
            var expectedScales = new[] { ((decimal)initialAmount).Scale, ((decimal)changedAmount).Scale };
            var expectedTruncatedScales = new[] { ((decimal)(long)initialAmount).Scale, ((decimal)(long)changedAmount).Scale };
            using var exactSubscription = model.WhenValueChanged(static price => ((decimal)price.Amount).Scale)
                .RecordValues(out var exact);
            using var truncatedSubscription = model.WhenValueChanged(static price => ((decimal)(long)price.Amount).Scale)
                .RecordValues(out var truncated);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(exact.Error).IsNull().Because("the direct conversion remains observable");
            await Assert.That(truncated.Error).IsNull().Because("the conversion through an integer remains observable");
            await Assert.That(exact.RecordedValues).IsEquivalentTo(expectedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the direct path preserves fractional digits");
            await Assert.That(truncated.RecordedValues).IsEquivalentTo(expectedTruncatedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the integer conversion discards fractional digits");
            await Assert.That(exact.HasCompleted).IsFalse().Because("the direct observation remains active");
            await Assert.That(truncated.HasCompleted).IsFalse().Because("the integer-converted observation remains active");
        }

        /// <summary>Verifies that conversions to different result types do not share incompatible property factories.</summary>
        [Test]
        public async Task LeafConversionsWithDifferentResultTypes_Changes_UseEachResultType()
        {
            // Arrange
            var initialAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var changedAmount = initialAmount + _randomizer.Int(1, ushort.MaxValue);
            var model = new ObservablePrice { Amount = initialAmount };
            var expectedDecimals = new[] { (decimal)initialAmount, (decimal)changedAmount };
            var expectedIntegers = new[] { (long)initialAmount, (long)changedAmount };
            using var decimalSubscription = model.WhenValueChanged(static price => (decimal)price.Amount)
                .RecordValues(out var decimals);
            using var integerSubscription = model.WhenValueChanged(static price => (long)price.Amount)
                .RecordValues(out var integers);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(decimals.Error).IsNull().Because("the decimal result must have a compatible factory");
            await Assert.That(integers.Error).IsNull().Because("the integer result must have a compatible factory");
            await Assert.That(decimals.RecordedValues).IsEquivalentTo(expectedDecimals, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the decimal path preserves fractional values");
            await Assert.That(integers.RecordedValues).IsEquivalentTo(expectedIntegers, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the integer path truncates fractional values");
            await Assert.That(decimals.HasCompleted).IsFalse().Because("the decimal observation remains active");
            await Assert.That(integers.HasCompleted).IsFalse().Because("the integer observation remains active");
        }

        /// <summary>Verifies that conversion methods with matching source and result types retain distinct behavior.</summary>
        [Test]
        public async Task ConversionMethodsWithSameSignature_Changes_UseEachMethod()
        {
            // Arrange
            var initialAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 4d;
            var changedAmount = (_randomizer.Int(1, ushort.MaxValue) | 1) / 8d;
            var model = new ObservablePrice { Amount = initialAmount };
            Func<double, decimal> exactConversion = ConvertAmount;
            Func<double, decimal> truncatedConversion = TruncateAmount;
            var parameter = Expression.Parameter(typeof(ObservablePrice), nameof(model));
            var amount = Expression.Property(parameter, nameof(ObservablePrice.Amount));
            var exactBody = Expression.Property(Expression.Convert(amount, typeof(decimal), exactConversion.Method), nameof(decimal.Scale));
            var truncatedBody = Expression.Property(
                Expression.Convert(amount, typeof(decimal), truncatedConversion.Method), nameof(decimal.Scale));
            var exactPath = Expression.Lambda<Func<ObservablePrice, byte>>(exactBody, parameter);
            var truncatedPath = Expression.Lambda<Func<ObservablePrice, byte>>(truncatedBody, parameter);
            var expectedScales = new[] { exactConversion(initialAmount).Scale, exactConversion(changedAmount).Scale };
            var expectedTruncatedScales = new[] { truncatedConversion(initialAmount).Scale, truncatedConversion(changedAmount).Scale };
            using var exactSubscription = model.WhenValueChanged(exactPath)
                .RecordValues(out var exact);
            using var truncatedSubscription = model.WhenValueChanged(truncatedPath)
                .RecordValues(out var truncated);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(exact.Error).IsNull().Because("the fractional conversion method remains observable");
            await Assert.That(truncated.Error).IsNull().Because("the truncating conversion method remains observable");
            await Assert.That(exact.RecordedValues).IsEquivalentTo(expectedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the fractional conversion method belongs to this path");
            await Assert.That(truncated.RecordedValues).IsEquivalentTo(expectedTruncatedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the truncating conversion method belongs to this path");
            await Assert.That(exact.HasCompleted).IsFalse().Because("the fractional observation remains active");
            await Assert.That(truncated.HasCompleted).IsFalse().Because("the truncating observation remains active");
        }

        /// <summary>Verifies that a user-defined conversion operator is applied before reading a property of its result.</summary>
        [Test]
        public async Task UserDefinedConversionBeforePropertyAccess_PropertyChanges_AreObserved()
        {
            // Arrange
            // A fractional amount proves the operator ran, because it keeps only whole units.
            var initialAmount = _randomizer.Int(1, ushort.MaxValue) + 0.5d;
            var changedAmount = initialAmount + _randomizer.Int(1, ushort.MaxValue);
            var model = new ObservablePrice { Amount = initialAmount };
            var expectedUnits = new[] { ((Money)initialAmount).WholeUnits, ((Money)changedAmount).WholeUnits };
            using var subscription = model.WhenValueChanged(static price => ((Money)price.Amount).WholeUnits)
                .RecordValues(out var results);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(results.Error).IsNull().Because("a user-defined conversion operator must run before the next property is read");
            await Assert.That(results.RecordedValues).IsEquivalentTo(expectedUnits, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("each observed value must be read from the converted result");
            await Assert.That(results.HasCompleted).IsFalse().Because("further amount changes remain observable");
        }

        /// <summary>Verifies that a user-defined conversion keeps its own evaluation path alongside a built-in conversion.</summary>
        [Test]
        public async Task UserDefinedAndBuiltInConversionsOnSameProperty_Changes_UseEachConversion()
        {
            // Arrange
            var initialAmount = _randomizer.Int(1, ushort.MaxValue) + 0.5d;
            var changedAmount = initialAmount + _randomizer.Int(1, ushort.MaxValue);
            var model = new ObservablePrice { Amount = initialAmount };
            var expectedUnits = new[] { ((Money)initialAmount).WholeUnits, ((Money)changedAmount).WholeUnits };
            var expectedScales = new[] { ((decimal)initialAmount).Scale, ((decimal)changedAmount).Scale };
            using var moneySubscription = model.WhenValueChanged(static price => ((Money)price.Amount).WholeUnits)
                .RecordValues(out var units);
            using var decimalSubscription = model.WhenValueChanged(static price => ((decimal)price.Amount).Scale)
                .RecordValues(out var scales);

            // Act
            model.Amount = changedAmount;

            // Assert
            await Assert.That(units.Error).IsNull().Because("the user-defined conversion must have a compatible factory");
            await Assert.That(scales.Error).IsNull().Because("the built-in conversion must have a compatible factory");
            await Assert.That(units.RecordedValues).IsEquivalentTo(expectedUnits, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the user-defined path reports whole units");
            await Assert.That(scales.RecordedValues).IsEquivalentTo(expectedScales, TUnit.Assertions.Enums.CollectionOrdering.Matching).Because("the built-in path reports the decimal scale");
            await Assert.That(units.HasCompleted).IsFalse().Because("the user-defined observation remains active");
            await Assert.That(scales.HasCompleted).IsFalse().Because("the built-in observation remains active");
        }

        /// <summary>Verifies that equivalent converted paths share a factory regardless of lambda parameter names.</summary>
        [Test]
        public async Task EquivalentConversionPaths_DifferentParameterNames_ReuseFactory()
        {
            // Arrange
            Expression<Func<ObservablePrice, byte>> first = static price => ((decimal)price.Amount).Scale;
            Expression<Func<ObservablePrice, byte>> second = static otherPrice => ((decimal)otherPrice.Amount).Scale;
            var firstFactory = ObservablePropertyFactoryCache.Instance.GetFactory(first);

            // Act
            var secondFactory = ObservablePropertyFactoryCache.Instance.GetFactory(second);

            // Assert
            await Assert.That(secondFactory).IsSameReferenceAs(firstFactory).Because("parameter names do not change a property's evaluation path");
        }
    }
}
