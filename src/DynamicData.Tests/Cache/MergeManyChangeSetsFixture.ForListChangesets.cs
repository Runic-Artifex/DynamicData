using System;
using System.Collections.Generic;
using System.Linq;

using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public static partial class MergeManyChangeSetsFixture
{
    public static partial class ForListChangesets
    {
        #if DEBUG
        private const int InitialOwnerCount = 7;
        private const int AddRangeSize = 5;
        private const int RemoveRangeSize = 3;
        #else
        private const int InitialOwnerCount = 103;
        private const int AddRangeSize = 53;
        private const int RemoveRangeSize = 37;
        #endif

        private static async Task CheckResultContents(
            IReadOnlyList<AnimalOwner> owners,
            ChangeSetAggregator<AnimalOwner, Guid> ownerResults,
            ChangeSetAggregator<Animal> animalResults)
        {
            var expectedOwners = owners.ToList();

            // These should be subsets of each other
            await Assert.That(expectedOwners.Except(ownerResults.Data.Items).Any()).IsFalse();
            await Assert.That(ownerResults.Data.Items.Count).IsEqualTo(expectedOwners.Count);

            // All owner animals should be in the results
            foreach (var owner in owners)
            {
                await Assert.That(owner.Animals.Items.Except(animalResults.Data.Items).Any()).IsFalse();
            }

            // Results should not have more than the total number of animals
            await Assert.That(animalResults.Data.Count).IsEqualTo(owners.Sum(owner => owner.Animals.Count));
        }
    }
}
