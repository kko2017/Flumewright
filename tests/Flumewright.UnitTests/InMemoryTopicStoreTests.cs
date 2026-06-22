using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class InMemoryTopicStoreTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public async Task Publish_AssignsIncreasingOffsets()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        var res1 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);
        var res2 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);
        var res3 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        res1.Offset.Should().Be(0);
        res2.Offset.Should().Be(1);
        res3.Offset.Should().Be(2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_ReceivesPublishedMessage()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string> { ["key"] = "value" };
        var payload = new byte[] { 1, 2, 3 }.AsMemory();
        using var cts = new CancellationTokenSource();

        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        await Task.Delay(50);
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        var hasNext = await pendingRead;
        hasNext.Should().BeTrue();
        
        var msg = enumerator.Current;
        msg.Offset.Should().Be(0);
        msg.Headers.Should().BeEquivalentTo(headers);
        msg.Payload.ToArray().Should().BeEquivalentTo(payload.ToArray());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_LatestSemantics_StartsFromEnd()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        // Publish offset 0
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        using var cts = new CancellationTokenSource();
        // Subscribe with default (starts from end)
        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        // Give the background reader tasks a moment to initialize and resolve LATEST
        // before we publish the message we want them to catch.
        await Task.Delay(50);

        // Publish offset 1
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        var hasNext = await pendingRead;
        hasNext.Should().BeTrue();
        
        // Reader should see offset 1, not 0
        enumerator.Current.Offset.Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Payload_RoundTripsByteExact()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string>();
        var payload = new byte[] { 255, 0, 128, 64, 32 }.AsMemory();
        
        using var cts = new CancellationTokenSource();
        var enumerator = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        await Task.Delay(50);
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        await pendingRead;
        var received = enumerator.Current.Payload;

        received.ToArray().Should().BeEquivalentTo(payload.ToArray());
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SeparateTopics_AreIsolated()
    {
        var store = new InMemoryTopicStore(1);
        using var cts = new CancellationTokenSource();
        
        var enumeratorA = store.SubscribeAsync("A", cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingReadA = enumeratorA.MoveNextAsync().AsTask();

        await Task.Delay(50);
        await store.PublishAsync("B", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var delayTask = Task.Delay(50);
        var completedTask = await Task.WhenAny(pendingReadA, delayTask);

        completedTask.Should().Be(delayTask, "topic A should not receive messages from topic B");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Offsets_AreUniqueAndContiguousUnderConcurrentPublishes()
    {
        var store = new InMemoryTopicStore(1);
        int count = 1000;
        var tasks = new Task<(int, long)>[count];

        for (int i = 0; i < count; i++)
        {
            tasks[i] = store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty).AsTask();
        }

        var results = await Task.WhenAll(tasks);
        var offsets = results.Select(r => r.Item2).ToArray();

        var uniqueOffsets = offsets.Distinct().OrderBy(x => x).ToList();
        uniqueOffsets.Count.Should().Be(count);
        uniqueOffsets.First().Should().Be(0);
        uniqueOffsets.Last().Should().Be(count - 1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task FanOut_TwoSubscribersReceiveSameMessage()
    {
        var store = new InMemoryTopicStore(1);
        using var cts = new CancellationTokenSource();

        var enum1 = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var enum2 = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        
        var read1 = enum1.MoveNextAsync().AsTask();
        var read2 = enum2.MoveNextAsync().AsTask();

        await Task.Delay(50);
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), new byte[] { 42 }.AsMemory());

        var hasNext1 = await read1;
        var hasNext2 = await read2;

        hasNext1.Should().BeTrue();
        hasNext2.Should().BeTrue();

        enum1.Current.Offset.Should().Be(0);
        enum2.Current.Offset.Should().Be(0);
        enum1.Current.Payload.ToArray()[0].Should().Be(42);
        enum2.Current.Payload.ToArray()[0].Should().Be(42);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task LateSubscriber_DoesNotGetPreSubscriptionMessages()
    {
        var store = new InMemoryTopicStore(1);
        using var cts = new CancellationTokenSource();

        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), new byte[] { 1 }.AsMemory());

        var enum1 = store.SubscribeAsync("topic1", cts.Token).GetAsyncEnumerator(cts.Token);
        var read1 = enum1.MoveNextAsync().AsTask();

        await Task.Delay(50);
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), new byte[] { 2 }.AsMemory());

        var hasNext1 = await read1;
        hasNext1.Should().BeTrue();

        enum1.Current.Offset.Should().Be(1);
        enum1.Current.Payload.ToArray()[0].Should().Be(2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Unsubscribe_CleansUpAndOthersStillWork()
    {
        var store = new InMemoryTopicStore(1);
        
        var cts1 = new CancellationTokenSource();
        var cts2 = new CancellationTokenSource();

        var enum1 = store.SubscribeAsync("topic1", cts1.Token).GetAsyncEnumerator(cts1.Token);
        var enum2 = store.SubscribeAsync("topic1", cts2.Token).GetAsyncEnumerator(cts2.Token);
        
        var read1 = enum1.MoveNextAsync().AsTask();
        var read2 = enum2.MoveNextAsync().AsTask();

        await Task.Delay(50);

        // Cancel subscriber 1
        await cts1.CancelAsync();

        // Ensure enum1 throws TaskCanceledException or returns false
        try
        {
            await read1;
        }
        catch (OperationCanceledException) { }

        // Publish
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), new byte[] { 99 }.AsMemory());

        // Enum2 should still get it
        var hasNext2 = await read2;
        hasNext2.Should().BeTrue();
        enum2.Current.Payload.ToArray()[0].Should().Be(99);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task SamePartitionKey_LandsInSamePartition_OffsetsIncrease()
    {
        var store = new InMemoryTopicStore(4);
        var key = new byte[] { 1, 2, 3 }.AsMemory();
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        var res1 = await store.PublishAsync("topic1", key, headers, payload);
        var res2 = await store.PublishAsync("topic1", key, headers, payload);

        res1.Partition.Should().Be(res2.Partition);
        res1.Offset.Should().Be(0);
        res2.Offset.Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Offsets_ArePerPartitionIndependent()
    {
        var store = new InMemoryTopicStore(4);
        int? p1 = null;
        int? p2 = null;
        byte[] key1 = null!;
        byte[] key2 = null!;
        
        for (byte i = 0; i < 100; i++)
        {
            var keyBytes = new byte[] { i };
            int p = PartitionRouter.ForKey(keyBytes, 4);
            if (p1 == null)
            {
                p1 = p;
                key1 = keyBytes;
            }
            else if (p != p1.Value && p2 == null)
            {
                p2 = p;
                key2 = keyBytes;
                break;
            }
        }

        p1.Should().NotBeNull();
        p2.Should().NotBeNull();
        p1.Value.Should().NotBe(p2.Value);

        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        var res1 = await store.PublishAsync("topic1", key1.AsMemory(), headers, payload);
        var res2 = await store.PublishAsync("topic1", key2.AsMemory(), headers, payload);

        res1.Partition.Should().Be(p1.Value);
        res1.Offset.Should().Be(0);

        res2.Partition.Should().Be(p2.Value);
        res2.Offset.Should().Be(0);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task RoundRobin_NoKey_SpreadsAcrossPartitions()
    {
        var store = new InMemoryTopicStore(3);
        var headers = new Dictionary<string, string>();
        var payload = ReadOnlyMemory<byte>.Empty;

        var res1 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);
        var res2 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);
        var res3 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);
        var res4 = await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload);

        var partitions = new[] { res1.Partition, res2.Partition, res3.Partition, res4.Partition };
        partitions[3].Should().Be(partitions[0]);
        partitions[1].Should().NotBe(partitions[0]);
        partitions[2].Should().NotBe(partitions[1]);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ReadPartition_ReturnsRecordsInAppendOrder()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string>();
        var payload1 = new byte[] { 10 }.AsMemory();
        var payload2 = new byte[] { 20 }.AsMemory();

        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload1);
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload2);

        using var cts = new CancellationTokenSource();
        var enumerator = store.ReadPartitionAsync("topic1", 0, 0, cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).Should().BeTrue();
        enumerator.Current.Offset.Should().Be(0);
        enumerator.Current.Payload.ToArray()[0].Should().Be(10);

        (await enumerator.MoveNextAsync()).Should().BeTrue();
        enumerator.Current.Offset.Should().Be(1);
        enumerator.Current.Payload.ToArray()[0].Should().Be(20);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Reader_StartingAtZero_SeesRetainedEarlierRecords()
    {
        var store = new InMemoryTopicStore(1);
        var headers = new Dictionary<string, string>();
        var payload1 = new byte[] { 100 }.AsMemory();

        // Publish before subscribing
        await store.PublishAsync("topic1", ReadOnlyMemory<byte>.Empty, headers, payload1);

        using var cts = new CancellationTokenSource();
        // Subscribe from offset 0
        var enumerator = store.SubscribeAsync("topic1", 0, cts.Token).GetAsyncEnumerator(cts.Token);
        var pendingRead = enumerator.MoveNextAsync().AsTask();

        var hasNext = await pendingRead;
        hasNext.Should().BeTrue();
        
        enumerator.Current.Offset.Should().Be(0);
        enumerator.Current.Payload.ToArray()[0].Should().Be(100);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_FaultedReader_PropagatesExceptionToSubscriber()
    {
        // Arrange
        var store = new InMemoryTopicStore(1);
        
        // Use reflection to inject a null messages list in the Partition object
        // so that it throws a NullReferenceException when ReadFromOffsetAsync is evaluated.
        var topicType = typeof(InMemoryTopicStore).GetNestedType("Topic", System.Reflection.BindingFlags.NonPublic);
        var partitionType = typeof(InMemoryTopicStore).GetNestedType("Partition", System.Reflection.BindingFlags.NonPublic);
        
        Assert.NotNull(topicType);
        Assert.NotNull(partitionType);
        
        var topicInstance = Activator.CreateInstance(topicType, 1);
        var partitionsField = topicType.GetProperty("Partitions", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(partitionsField);
        var partitionsArray = (Array)partitionsField.GetValue(topicInstance)!;
        var partitionInstance = partitionsArray.GetValue(0);
        
        var messagesField = partitionType.GetField("_messages", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(messagesField);
        messagesField.SetValue(partitionInstance, null);
        
        var topicsField = typeof(InMemoryTopicStore).GetField("_topics", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(topicsField);
        var topicsDict = (System.Collections.IDictionary)topicsField.GetValue(store)!;
        Assert.NotNull(topicsDict);
        topicsDict.Add("faulted-topic", topicInstance);

        using var cts = new CancellationTokenSource();
        
        // Act & Assert
        // The exception should propagate and be thrown during MoveNextAsync on the subscriber.
        var enumerator = store.SubscribeAsync("faulted-topic", 0, cts.Token).GetAsyncEnumerator(cts.Token);
        
        Func<Task> act = async () =>
        {
            await enumerator.MoveNextAsync();
        };

        // Channel.Reader.WaitToReadAsync throws the exception passed to TryComplete.
        // Since Task.WhenAll wraps the exception in an AggregateException, it might be an AggregateException.
        var exception = await act.Should().ThrowAsync<Exception>();
        var ex = exception.And;
        bool isExpected = ex is NullReferenceException || 
            (ex is AggregateException agg && agg.InnerExceptions.Any(ie => ie is NullReferenceException));
            
        isExpected.Should().BeTrue();
    }
}
