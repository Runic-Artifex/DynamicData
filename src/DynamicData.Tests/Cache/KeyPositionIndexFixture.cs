#if REACTIVE_TESTS
using DynamicData.Reactive.Cache.Internal;
#else
using DynamicData.Cache.Internal;
#endif

namespace DynamicData.Tests.Cache;

public class KeyPositionIndexFixture
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MonotoneInsertionsAndAlternatingEndRemovalsMaintainRanks(bool insertAtFront)
    {
        const int count = 10_000;
        var positions = new KeyPositionIndex<int>();
        for (var key = 0; key < count; ++key)
            positions.InsertAt(insertAtFront ? 0 : positions.Count, key);
        var first = 0;
        var last = count - 1;
        for (var operation = 0; operation < count; ++operation)
        {
            var removeFromFront = operation % 2 == 0;
            var modelIndex = removeFromFront ? first++ : last--;
            var key = insertAtFront ? count - modelIndex - 1 : modelIndex;
            var expectedRank = removeFromFront ? 0 : positions.Count - 1;
            await Assert.That(positions.TryGetIndex(key, out var rank)).IsTrue();
            await Assert.That(rank).IsEqualTo(expectedRank);
            await Assert.That(positions.Remove(key)).IsEqualTo(expectedRank);
            await Assert.That(positions.Contains(key)).IsFalse();
            await Assert.That(positions.Count).IsEqualTo(count - operation - 1);
        }

        await Assert.That(positions.Remove(-1)).IsEqualTo(-1);
        // Reuse after the last root was removed, including a previously removed key.
        positions.InsertAt(0, 1);
        await Assert.That(positions.TryGetIndex(1, out var finalRank)).IsTrue();
        await Assert.That(finalRank).IsEqualTo(0);
        await Assert.That(positions.Remove(1)).IsEqualTo(0);
    }
}
