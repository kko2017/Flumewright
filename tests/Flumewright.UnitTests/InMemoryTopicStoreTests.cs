using FluentAssertions;
using Flumewright.Broker.Core;

namespace Flumewright.UnitTests;

public class InMemoryTopicStoreTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public async Task Publish_AssignsIncreasingOffsets()
    {
        var store = new InMemoryTopicStore();
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        var offset1 = await store.PublishAsync("topic1", headers, payload);
        var offset2 = await store.PublishAsync("topic1", headers, payload);
        var offset3 = await store.PublishAsync("topic1", headers, payload);

        offset1.Should().Be(0);
        offset2.Should().Be(1);
        offset3.Should().Be(2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_ReceivesPublishedMessage()
    {
        var store = new InMemoryTopicStore();
        var headers = new Dictionary<string, string> { ["key"] = "value" };
        var payload = new byte[] { 1, 2, 3 }.AsMemory();
        using var cts = new CancellationTokenSource();

        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        await store.PublishAsync("topic1", headers, payload);

        var hasNext = await pendingRead;
        hasNext.Should().BeTrue();
        
        var msg = enumerator.Current;
        msg.Offset.Should().Be(0);
        msg.Headers.Should().BeEquivalentTo(headers);
        msg.Payload.ToArray().Should().BeEquivalentTo(payload.ToArray());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_LatestSemantics_DropsOldMessages()
    {
        var store = new InMemoryTopicStore();
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        await store.PublishAsync("topic1", headers, payload);

        using var cts = new CancellationTokenSource();
        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        await store.PublishAsync("topic1", headers, payload);

        var hasNext = await pendingRead;
        hasNext.Should().BeTrue();
        
        enumerator.Current.Offset.Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Payload_RoundTripsByteExact()
    {
        var store = new InMemoryTopicStore();
        var headers = new Dictionary<string, string>();
        var payload = new byte[] { 255, 0, 128, 64, 32 }.AsMemory();
        
        using var cts = new CancellationTokenSource();
        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        await store.PublishAsync("topic1", headers, payload);

        await pendingRead;
        var received = enumerator.Current.Payload;

        received.ToArray().Should().BeEquivalentTo(payload.ToArray());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SeparateTopics_AreIsolated()
    {
        var store = new InMemoryTopicStore();
        using var cts = new CancellationTokenSource();
        
        var enumeratorA = store.SubscribeAsync("A", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingReadA = enumeratorA.MoveNextAsync().AsTask();

        await store.PublishAsync("B", new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var delayTask = Task.Delay(50);
        var completedTask = await Task.WhenAny(pendingReadA, delayTask);

        completedTask.Should().Be(delayTask, "topic A should not receive messages from topic B");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Offsets_AreUniqueUnderConcurrentPublishes()
    {
        var store = new InMemoryTopicStore();
        int count = 1000;
        var tasks = new Task<long>[count];

        for (int i = 0; i < count; i++)
        {
            tasks[i] = store.PublishAsync("topic1", new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty).AsTask();
        }

        var offsets = await Task.WhenAll(tasks);

        var uniqueOffsets = offsets.Distinct().OrderBy(x => x).ToList();
        uniqueOffsets.Count.Should().Be(count);
        uniqueOffsets.First().Should().Be(0);
        uniqueOffsets.Last().Should().Be(count - 1);
    }
}
