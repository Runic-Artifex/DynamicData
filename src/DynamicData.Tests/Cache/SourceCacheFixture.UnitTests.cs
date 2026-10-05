using System;
using System.Collections.Generic;
using System.Linq;

using DynamicData.Tests.Domain;

namespace DynamicData.Tests.Cache;

public static partial class SourceCacheFixture
{
    public class UnitTests
    {
        [Test]
        public async Task CanHandleABatchOfUpdates()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);
            using var results = source.Connect().AsAggregator();

            source.Edit(
                updater =>
                {
                    var torequery = new Person("Adult1", 44);

                    updater.AddOrUpdate(new Person("Adult1", 40));
                    updater.AddOrUpdate(new Person("Adult1", 41));
                    updater.AddOrUpdate(new Person("Adult1", 42));
                    updater.AddOrUpdate(new Person("Adult1", 43));
                    updater.Refresh(torequery);
                    updater.Remove(torequery);
                    updater.Refresh(torequery);
                });

            await Assert.That(results.Summary.Overall.Count).IsEqualTo(6).Because("Should be  6 up`dates");
            await Assert.That(results.Messages.Count).IsEqualTo(1).Because("Should be 1 message");
            await Assert.That(results.Messages[0].Adds).IsEqualTo(1).Because("Should be 1 update");
            await Assert.That(results.Messages[0].Updates).IsEqualTo(3).Because("Should be 3 updates");
            await Assert.That(results.Messages[0].Removes).IsEqualTo(1).Because("Should be  1 remove");
            await Assert.That(results.Messages[0].Refreshes).IsEqualTo(1).Because("Should be 1 evaluate");

            await Assert.That(results.Data.Count).IsEqualTo(0).Because("Should be 1 item in` the cache");
        }

        [Test]
        public async Task ConnectContinuesToWorkNormallyAfterAFailedEdit()
        {
            using var source = new SourceCache<int, int>(static item => item);

            source.AddOrUpdate(1);

            var thrown5 = await Assert.That((Action)(() => source.Edit(_ => throw new Exception("Test")))).Throws<Exception>();
            await Assert.That(thrown5.Message).IsEqualTo("Test");

            using var subscription = source.Connect().RecordCacheItems(out var results);

            await Assert.That(results.Error).IsNull().Because("new subscribers should not receive previous errors");
            await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(1).Because("an initial changeset should have been published.");
        }

        // Covers https://github.com/reactivemarbles/DynamicData/issues/1129
        [Test]
        public async Task ConnectDuringEditsDoesNotDuplicate()
        {
            using var items = new SourceCache<int, int>(static item => item);

            using var subscriptions = new CompositeDisposable();

            // An initial subscription is required to initiate internal buffering of changes, during the upcoming .Edit().
            // That is, we want there to be changes buffered, internally, when the mid-edit subscription comes in, to
            // ensure that they don't get duplicated. This is the scenario that came in up #1129.
            subscriptions.Add(items
                .Connect()
                .Subscribe());

            CacheItemRecordingObserver<int, int>? results = null;
            var midEditObservations = new List<(Exception? Error, int ChangeSets)>();

            items.Edit(inner =>
            {
                inner.AddOrUpdate(1);

                subscriptions.Add(items
                    .Connect()
                    .ValidateChangeSets(static item => item)
                    .RecordCacheItems(out results));

                midEditObservations.Add((results.Error, results.RecordedChangeSets.Count));

                inner.AddOrUpdate(2);

                midEditObservations.Add((results.Error, results.RecordedChangeSets.Count));

                // Explicitly doing a nested edit, as that system is closely intertwined with the edit-tracking system that
                // .Connect() uses.
                items.Remove(item: 1);

                midEditObservations.Add((results.Error, results.RecordedChangeSets.Count));
            });

            await Assert.That(midEditObservations).HasCount(3);
            foreach (var observation in midEditObservations)
            {
                await Assert.That(observation.Error).IsNull();
                await Assert.That(observation.ChangeSets).IsEqualTo(0).Because("connect must not publish in the middle of an edit");
            }

            await Assert.That(results).IsNotNull().Because("the edit delegate should have been invoked");
            await Assert.That(results.Error).IsNull().Because("no errors should have occurred");
            await Assert.That(results.RecordedChangeSets.Count).IsEqualTo(1).Because("subscribers should only receive a single initial changeset");
            await Assert.That(results.RecordedItemsByKey).IsEquivalentTo(new Dictionary<int, int> { [2] = 2 }, TUnit.Assertions.Enums.CollectionOrdering.Any);

            await Assert.That(results.HasCompleted).IsFalse().Because("the source has not yet completed");
        }

        [Test]
        public async Task CountChanged()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);
            using var results = source.Connect().AsAggregator();

            var count = 0;
            var invoked = 0;
            using (source.CountChanged.Subscribe(
                       c =>
                       {
                           count = c;
                           invoked++;
                       }))
            {
                await Assert.That(invoked).IsEqualTo(1);
                await Assert.That(count).IsEqualTo(0);

                source.AddOrUpdate(new RandomPersonGenerator().Take(100));
                await Assert.That(invoked).IsEqualTo(2);
                await Assert.That(count).IsEqualTo(100);

                source.Clear();
                await Assert.That(invoked).IsEqualTo(3);
                await Assert.That(count).IsEqualTo(0);
            }
        }

        [Test]
        public async Task CountChangedShouldAlwaysInvokeUponSubscription()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);

            int? result = null;
            var subscription = source.CountChanged.Subscribe(count => result = count);

            await Assert.That(result.HasValue).IsTrue();

            if (result is null)
            {
                throw new InvalidOperationException(nameof(result));
            }

            await Assert.That(result.Value).IsEqualTo(0).Because("Count should be zero");

            subscription.Dispose();
        }

        [Test]
        public async Task CountChangedShouldReflectContentsOfCacheInvokeUponSubscription()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);

            var generator = new RandomPersonGenerator();
            int? result = null;
            var subscription = source.CountChanged.Subscribe(count => result = count);

            source.AddOrUpdate(generator.Take(100));

            if (result is null)
            {
                throw new InvalidOperationException(nameof(result));
            }

            await Assert.That(result.HasValue).IsTrue();
            await Assert.That(result.Value).IsEqualTo(100).Because("Count should be 100");
            subscription.Dispose();
        }

        [Test]
        public async Task EmptyChanges()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);

            IChangeSet<Person, string>? change = null;

            using var subscription = source.Connect(suppressEmptyChangeSets: false)
                .Subscribe(c=> change = c);

            await Assert.That(change).IsNotNull();
            await Assert.That(change!.Count).IsEqualTo(0);
        }

        [Test]
        public async Task EmptyChangesWithFilter()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);

            IChangeSet<Person, string>? change = null;

            using var subscription = source.Connect(p=>p.Age == 20, suppressEmptyChangeSets: false)
                .Subscribe(c => change = c);

            await Assert.That(change).IsNotNull();
            await Assert.That(change!.Count).IsEqualTo(0);
        }

        [Test]
        public async Task StaticFilterRemove()
        {
            var cache = new SourceCache<SomeObject, int>(x => x.Id);

            var above5 = cache.Connect(x => x.Value > 5).AsObservableCache();
            var below5 = cache.Connect(x => x.Value <= 5).AsObservableCache();

            cache.AddOrUpdate(Enumerable.Range(1,10).Select(i=> new SomeObject(i,i)));

            await Assert.That(above5.Items).IsEquivalentTo(Enumerable.Range(6, 5).Select(i => new SomeObject(i, i)), TUnit.Assertions.Enums.CollectionOrdering.Any);
            await Assert.That(below5.Items).IsEquivalentTo(Enumerable.Range(1, 5).Select(i => new SomeObject(i, i)), TUnit.Assertions.Enums.CollectionOrdering.Any);

            //should move from above 5 to below 5
            cache.AddOrUpdate(new SomeObject(6,-1));

            await Assert.That(above5.Count).IsEqualTo(4);
            await Assert.That(below5.Count).IsEqualTo(6);

            await Assert.That(above5.Items).IsEquivalentTo(Enumerable.Range(7, 4).Select(i => new SomeObject(i, i)), TUnit.Assertions.Enums.CollectionOrdering.Any);
            await Assert.That(below5.Items).IsEquivalentTo(Enumerable.Range(1, 6).Select(i => new SomeObject(i, i == 6 ? -1 : i)), TUnit.Assertions.Enums.CollectionOrdering.Any);
        }

        [Test]
        public async Task SubscribeDisposesCorrectly()
        {
            using var source = new SourceCache<Person, string>(p => p.Key);

            var called = false;
            var errored = false;
            var completed = false;
            var subscription = source.Connect().Finally(() => completed = true).Subscribe(updates => { called = true; }, ex => errored = true, () => completed = true);
            source.AddOrUpdate(new Person("Adult1", 40));

            subscription.Dispose();
            source.Dispose();

            await Assert.That(errored).IsFalse();
            await Assert.That(called).IsTrue();
            await Assert.That(completed).IsTrue();
        }
    }
}
