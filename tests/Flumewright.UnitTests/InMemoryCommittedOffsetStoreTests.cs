using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class InMemoryCommittedOffsetStoreTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public async Task GetCommittedOffset_InitialState_ReturnsNull()
    {
        var topicStore = new InMemoryTopicStore(1);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);

        var v1 = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        v1.Should().BeNull();

        var v2 = await offsetStore.GetCommittedOffsetAsync("g2", "t1", 0);
        v2.Should().BeNull();
    }
    [Fact]
    [Trait("Category", "Unit")]
    public async Task ForwardCommitUpdates_ReadBackReturnsCommittedValue()
    {
        var topicStore = new InMemoryTopicStore(1);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        
        var result = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 1);
        result.Ok.Should().BeTrue();

        var value = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        value.Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task BackwardsCommitRejected_StoredValueUnchanged()
    {
        var topicStore = new InMemoryTopicStore(1);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        
        await offsetStore.CommitOffsetAsync("g1", "t1", 0, 1);

        var rejectResult = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 0);
        rejectResult.Ok.Should().BeFalse();
        rejectResult.Reason.Should().NotBeEmpty();

        var value = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        value.Should().Be(1);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task NegativeOffsetRejected()
    {
        var topicStore = new InMemoryTopicStore(1);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);

        var rejectNegative = await offsetStore.CommitOffsetAsync("g1", "t1", 0, -1);
        rejectNegative.Ok.Should().BeFalse();
        rejectNegative.Reason.Should().Be("Offset cannot be negative");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task OutOfRangeCommitRejected()
    {
        var topicStore = new InMemoryTopicStore(1);
        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);

        // Unknown topic: never published, therefore no records read, so reject.
        var rejectUnknown = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 0);
        rejectUnknown.Ok.Should().BeFalse();
        rejectUnknown.Reason.Should().Be("Unknown topic or invalid partition");

        // Publish 1 message -> highWatermark=1
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        // Invalid partition on existing topic
        var rejectInvalidPartition = await offsetStore.CommitOffsetAsync("g1", "t1", 99, 0);
        rejectInvalidPartition.Ok.Should().BeFalse();
        rejectInvalidPartition.Reason.Should().Be("Unknown topic or invalid partition");

        // offset 1 is ACCEPTED ("1 record processed", valid [0, 1])
        var resultOneNow = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 1);
        resultOneNow.Ok.Should().BeTrue();

        // offset 2 is REJECTED
        var resultTwo = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 2);
        resultTwo.Ok.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IndependentKeys_DoNotInterfere()
    {
        var topicStore = new InMemoryTopicStore(1);
        for (int i = 0; i < 10; i++) 
        {
             await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
             await topicStore.PublishAsync("t2", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        }

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        
        // Commits to distinct combinations on partition 0: (g1, t1, 0), (g2, t1, 0), (g1, t2, 0), (g2, t2, 0)
        var res1 = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 5);
        res1.Ok.Should().BeTrue();
        var res2 = await offsetStore.CommitOffsetAsync("g2", "t1", 0, 3);
        res2.Ok.Should().BeTrue();
        var res3 = await offsetStore.CommitOffsetAsync("g1", "t2", 0, 4);
        res3.Ok.Should().BeTrue();
        var res4 = await offsetStore.CommitOffsetAsync("g2", "t2", 0, 2);
        res4.Ok.Should().BeTrue();

        var v1 = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        var v2 = await offsetStore.GetCommittedOffsetAsync("g2", "t1", 0);
        var v3 = await offsetStore.GetCommittedOffsetAsync("g1", "t2", 0);
        var v4 = await offsetStore.GetCommittedOffsetAsync("g2", "t2", 0);

        v1.Should().Be(5);
        v2.Should().Be(3);
        v3.Should().Be(4);
        v4.Should().Be(2);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task ConcurrentCommitsToSameKey_LeaveMapAtHighestValidOffset_NoLostUpdate()
    {
        var topicStore = new InMemoryTopicStore(1);
        for (int i=0; i<1000; i++) 
        {
             await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        }

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        
        var offsetsToCommit = Enumerable.Range(0, 1000).ToList();
        // Shuffle the offsets so they are not dispatched in purely ascending order.
        var rng = new Random(42);
        offsetsToCommit = offsetsToCommit.OrderBy(x => rng.Next()).ToList();

        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new List<Task>();
        
        // Committing 0 through 999. The highest valid value in this set is 999.
        // Because the lock prevents lost updates and backwards commits are rejected,
        // the final value in the map must be the maximum successfully committed valid value (999),
        // regardless of the exact thread execution order.
        foreach (int offsetToCommit in offsetsToCommit)
        {
            tasks.Add(Task.Run(async () => {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
                await gate.Task;
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                await offsetStore.CommitOffsetAsync("g1", "t1", 0, offsetToCommit);
            }));
        }

        // Release the gate to let all tasks race genuinely at the same time
        gate.SetResult();

        // Bounded wait prevents hanging tests in case of deadlock (FIX-008)
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));

        var finalValue = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        finalValue.Should().Be(999);
    }
}
