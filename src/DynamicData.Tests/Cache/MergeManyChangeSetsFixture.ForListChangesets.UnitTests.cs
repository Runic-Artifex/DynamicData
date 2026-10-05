#if REACTIVE_TESTS
using DynamicData.Reactive.Kernel;
#else
using DynamicData.Kernel;
#endif
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Bogus;

using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public static partial class MergeManyChangeSetsFixture
{
    public static partial class ForListChangesets
    {
        public class UnitTests
        {
            [Test]
            public async Task NullChecks()
            {
                // Arrange
                var emptyChangeSetObs = Observable.Empty<IChangeSet<int, int>>();
                var nullChangeSetObs = (IObservable<IChangeSet<int, int>>)null!;
                var emptyKeySelector = new Func<int, int, IObservable<IChangeSet<string>>>((_, _) => Observable.Empty<IChangeSet<string>>());
                var nullKeySelector = (Func<int, int, IObservable<IChangeSet<string>>>)null!;
                var emptySelector = new Func<int, IObservable<IChangeSet<string>>>(i => Observable.Empty<IChangeSet<string>>());
                var nullSelector = (Func<int, IObservable<IChangeSet<string>>>)null!;

                // Act
                var checkParam1 = () => nullChangeSetObs.MergeManyChangeSets(emptyKeySelector);
                var checkParam2 = () => emptyChangeSetObs.MergeManyChangeSets(nullKeySelector);
                var checkParam3 = () => nullChangeSetObs.MergeManyChangeSets(emptySelector);
                var checkParam4 = () => emptyChangeSetObs.MergeManyChangeSets(nullSelector);

                // Assert
                await Assert.That(emptyChangeSetObs).IsNotNull();
                await Assert.That(emptyKeySelector).IsNotNull();
                await Assert.That(emptySelector).IsNotNull();
                await Assert.That(nullChangeSetObs).IsNull();
                await Assert.That(nullKeySelector).IsNull();
                await Assert.That(nullSelector).IsNull();

                await Assert.That(checkParam1).Throws<ArgumentNullException>();
                await Assert.That(checkParam2).Throws<ArgumentNullException>();
                await Assert.That(checkParam3).Throws<ArgumentNullException>();
                await Assert.That(checkParam4).Throws<ArgumentNullException>();
            }

            [Test]
            [Arguments(false, false)]
            [Arguments(false, true)]
            [Arguments(true, false)]
            [Arguments(true, true)]
            public async Task ResultCompletesOnlyWhenSourceAndAllChildrenComplete(bool completeSource, bool completeChildren)
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                // Act
                animalOwners.Items.Skip(completeChildren ? 0 : 1).ForEach(owner => owner.Dispose());
                if (completeSource)
                {
                    animalOwners.Dispose();
                }

                // Assert
                await Assert.That(animalOwnerResults.IsCompleted).IsEqualTo(completeSource);
                await Assert.That(animalResults.IsCompleted).IsEqualTo(completeSource && completeChildren);
            }

            [Test]
            public async Task ResultContainsAllInitialChildren()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                // Act

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(1);
                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsChildrenFromAddedParents()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var addThis = animalOwnerFaker.Generate();

                // Act
                animalOwners.AddOrUpdate(addThis);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount + 1);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                foreach (var added in addThis.Animals.Items)
                {
                    await Assert.That(animalResults.Data.Items).Contains(added);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsChildrenAddedWithAddRange()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var initialCount = animalOwners.Items.Sum(owner => owner.Animals.Count);
                var totalAdded = new List<Animal>();

                // Act
                animalOwners.Items.ForEach(owner => owner.Animals.AddRange(animalFaker.Generate(AddRangeSize).With(added => totalAdded.AddRange(added))));

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(1 + InitialOwnerCount); // Initial + 1 for each Range Added
                foreach (var animal in totalAdded)
                {
                    await Assert.That(animalResults.Data.Items).Contains(animal);
                }
                await Assert.That(animalOwners.Items.Sum(owner => owner.Animals.Count)).IsEqualTo(initialCount + totalAdded.Count);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsChildrenAddedWithInsert()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var insertIndex = randomizer.Number(randomOwner.Animals.Items.Count);
                var insertThis = animalFaker.Generate();
                var initialCount = animalOwners.Items.Sum(owner => owner.Animals.Count);

                // Act
                randomOwner.Animals.Insert(insertIndex, insertThis);

                // Assert
                await Assert.That(randomOwner.Animals.Items.ElementAt(insertIndex)).IsEqualTo(insertThis);
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                await Assert.That(animalResults.Data.Items).Contains(insertThis);
                await Assert.That(animalOwners.Items.Sum(owner => owner.Animals.Count)).IsEqualTo(initialCount + 1);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsCorrectItemsAfterChildClear()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var removedAnimals = randomOwner.Animals.Items.ToList();

                // Act
                randomOwner.Animals.Clear();

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                await Assert.That(randomOwner.Animals.Count).IsEqualTo(0);
                foreach (var removed in removedAnimals)
                {
                    await Assert.That(animalResults.Data.Items).DoesNotContain(removed);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsCorrectItemsAfterChildReplacement()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var replaceThis = randomizer.ListItem(randomOwner.Animals.Items.ToList());
                var withThis = animalFaker.Generate();

                // Act
                randomOwner.Animals.Replace(replaceThis, withThis);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                await Assert.That(randomOwner.Animals.Items).DoesNotContain(replaceThis);
                await Assert.That(randomOwner.Animals.Items).Contains(withThis);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultContainsCorrectItemsAfterParentUpdate()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var replaceThis = randomizer.ListItem(animalOwners.Items.ToList());
                var withThis = CreateWithSameId(animalOwnerFaker, replaceThis);

                // Act
                animalOwners.AddOrUpdate(withThis);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount); // Owner Count should not change
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2); // 2 = Initial Add and one changeset with remove old items / add new items
                foreach (var removed in replaceThis.Animals.Items)
                {
                    await Assert.That(animalResults.Data.Items).DoesNotContain(removed);
                }
                foreach (var added in withThis.Animals.Items)
                {
                    await Assert.That(animalResults.Data.Items).Contains(added);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);

                replaceThis.Dispose();
            }

            [Test]
            public async Task ResultDoesNotContainChildrenFromParentsBatchRemoved()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var removeThese = randomizer.ListItems(animalOwners.Items.ToList(), RemoveRangeSize);

                // Act
                animalOwners.Remove(removeThese);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount - RemoveRangeSize);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                foreach (var removed in removeThese.SelectMany(owner => owner.Animals.Items))
                {
                    await Assert.That(animalResults.Data.Items).DoesNotContain(removed);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);

                removeThese.ForEach(owner => owner.Dispose());
            }

            [Test]
            [Timeout(10_000)]
            public async Task ResultDoesNotContainChildrenFromParentAddedAndRemovedWhileAnotherThreadIsDelivering(CancellationToken cancellationToken)
            {
                // Arrange
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                using var deliveringOwner = animalOwnerFaker.Generate();
                using var transientOwner = animalOwnerFaker.Generate().AddAnimals(animalFaker, 1, AddRangeSize);
                animalOwners.AddOrUpdate(deliveringOwner);

                var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var release = new ManualResetEventSlim();
                var parkNextDelivery = false;
                using var animalResults = animalOwners.Connect()
                    .MergeManyChangeSets(owner => owner.Animals.Connect())
                    .Do(_ =>
                    {
                        if (parkNextDelivery)
                        {
                            parkNextDelivery = false;
                            parked.SetResult();
                            release.Wait(cancellationToken);
                        }
                    })
                    .AsAggregator();

                // Park a delivery on another thread, so that both parent changes queue up behind it. The transient
                // owner's children are subscribed while its removal is already queued.
                parkNextDelivery = true;
                var delivering = Task.Run(() => deliveringOwner.Animals.Add(animalFaker.Generate()), cancellationToken);
                await parked.Task.WaitAsync(cancellationToken);
                animalOwners.AddOrUpdate(transientOwner);
                animalOwners.Remove(transientOwner);

                // Act
                release.Set();
                await delivering.WaitAsync(cancellationToken);

                // Assert
                await Assert.That(animalResults.Data.Items).IsEquivalentTo(deliveringOwner.Animals.Items, TUnit.Assertions.Enums.CollectionOrdering.Any);
            }

            [Test]
            public async Task ResultDoesNotContainChildrenFromParentsRemovedWithRemove()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var removeThis = randomizer.ListItem(animalOwners.Items.ToList());

                // Act
                animalOwners.Remove(removeThis);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount - 1);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                foreach (var removed in removeThis.Animals.Items)
                {
                    await Assert.That(animalResults.Data.Items).DoesNotContain(removed);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);

                removeThis.Dispose();
            }

            [Test]
            public async Task ResultDoesNotContainChildrenRemovedWithRemove()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var removeThis = randomizer.ListItem(randomOwner.Animals.Items.ToList());
                var initialCount = animalOwners.Items.Sum(owner => owner.Animals.Count);

                // Act
                randomOwner.Animals.Remove(removeThis);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                await Assert.That(animalResults.Data.Items).DoesNotContain(removeThis);
                await Assert.That(animalOwners.Items.Sum(owner => owner.Animals.Count)).IsEqualTo(initialCount - 1);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultDoesNotContainChildrenRemovedWithRemoveAt()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var removeIndex = randomizer.Number(randomOwner.Animals.Count - 1);
                var removeThis = randomOwner.Animals.Items.ElementAt(removeIndex);
                var initialCount = animalOwners.Items.Sum(owner => owner.Animals.Count);

                // Act
                randomOwner.Animals.RemoveAt(removeIndex);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                await Assert.That(animalResults.Data.Items).DoesNotContain(removeThis);
                await Assert.That(animalOwners.Items.Sum(owner => owner.Animals.Count)).IsEqualTo(initialCount - 1);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultDoesNotContainChildrenRemovedWithRemoveMany()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var removeCount = randomizer.Number(1, randomOwner.Animals.Count - 1);
                var removeThese = randomizer.ListItems(randomOwner.Animals.Items.ToList(), removeCount);

                // Act
                randomOwner.Animals.RemoveMany(removeThese);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                foreach (var removed in removeThese)
                {
                    await Assert.That(randomOwner.Animals.Items).DoesNotContain(removed);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultDoesNotContainChildrenRemovedWithRemoveRange()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var randomOwner = randomizer.ListItem(animalOwners.Items.ToList());
                var removeCount = randomizer.Number(1, randomOwner.Animals.Count - 1);
                var removeIndex = randomizer.Number(randomOwner.Animals.Count - removeCount - 1);
                var removeThese = randomOwner.Animals.Items.Skip(removeIndex).Take(removeCount);

                // Act
                randomOwner.Animals.RemoveRange(removeIndex, removeCount);

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(InitialOwnerCount);
                await Assert.That(animalResults.Messages.Count).IsEqualTo(2);
                foreach (var removed in removeThese)
                {
                    await Assert.That(randomOwner.Animals.Items).DoesNotContain(removed);
                }

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);
            }

            [Test]
            public async Task ResultEmptyIfSourceIsCleared()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                using var animalOwnerResults = animalOwners.Connect().AsAggregator();
                using var animalResults = animalOwners.Connect().MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                var items = animalOwners.Items.ToList();

                // Act
                animalOwners.Clear();

                // Assert
                await Assert.That(animalOwnerResults.Data.Count).IsEqualTo(0);
                await Assert.That(animalResults.Data.Count).IsEqualTo(0);

                await CheckResultContents(animalOwners.Items, animalOwnerResults, animalResults);

                items.ForEach(owner => owner.Dispose());
            }

            [Test]
            public async Task ResultFailsIfSourceFails()
            {
                using var animalOwners = new SourceCache<AnimalOwner, Guid>(o => o.Id);

                var randomizer = new Randomizer(0x01221948);
                var animalFaker = Fakers.Animal.Clone().WithSeed(randomizer);
                var animalOwnerFaker = Fakers.AnimalOwner.Clone().WithSeed(randomizer).WithInitialAnimals(animalFaker);

                // Arrange
                animalOwners.AddOrUpdate(animalOwnerFaker.Generate(InitialOwnerCount));

                var expectedError = new Exception("Expected");
                var throwObservable = Observable.Throw<IChangeSet<AnimalOwner, Guid>>(expectedError);
                using var results = animalOwners.Connect().Concat(throwObservable).MergeManyChangeSets(owner => owner.Animals.Connect()).AsAggregator();

                // Act
                animalOwners.Dispose();

                // Assert
                await Assert.That(results.Exception).IsEqualTo(expectedError);
            }

            private static AnimalOwner CreateWithSameId(
                Faker<AnimalOwner> animalOwnerFaker,
                AnimalOwner original)
            {
                var newOwner = animalOwnerFaker.Generate();
                var sameId = new AnimalOwner(newOwner.Name, original.Id);
                sameId.Animals.AddRange(newOwner.Animals.Items);
                return sameId;
            }
        }
    }
}
