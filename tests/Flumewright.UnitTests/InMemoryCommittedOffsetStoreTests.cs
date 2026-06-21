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
    public async Task OutOfRangeCommitRejected()
    {
        var topicStore = new InMemoryTopicStore(1);
        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);

        // Topic has 0 messages, so any offset >= 0 is out of range
        var result = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 0);
        result.Ok.Should().BeFalse();
        result.Reason.Should().NotBeEmpty();

        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        // Topic has 1 message (offset 0), so offset 1 is out of range
        var result2 = await offsetStore.CommitOffsetAsync("g1", "t1", 0, 1);
        result2.Ok.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task IndependentKeys_DoNotInterfere()
    {
        var topicStore = new InMemoryTopicStore(2);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);

        // We assume the messages got distributed to partitions 0 and 1, so both partitions have messages (at least 1, max 2)
        // For simplicity we use partition 0 for t1, partition 0 for t2
        await topicStore.PublishAsync("t2", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);

        await offsetStore.CommitOffsetAsync("g1", "t1", 0, 0);
        await offsetStore.CommitOffsetAsync("g2", "t1", 0, 1); // might be out of range if only 1 message went to p0, let's fix this by sending many.

        // Actually let's just make sure they have enough messages.
        for (int i=0; i<10; i++) {
             await topicStore.PublishAsync("t1", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
             await topicStore.PublishAsync("t2", ReadOnlyMemory<byte>.Empty, new Dictionary<string, string>(), ReadOnlyMemory<byte>.Empty);
        }
        
        await offsetStore.CommitOffsetAsync("g1", "t1", 0, 5);
        await offsetStore.CommitOffsetAsync("g2", "t1", 0, 3);
        await offsetStore.CommitOffsetAsync("g1", "t1", 1, 4);
        await offsetStore.CommitOffsetAsync("g1", "t2", 0, 2);

        var v1 = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        var v2 = await offsetStore.GetCommittedOffsetAsync("g2", "t1", 0);
        var v3 = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 1);
        var v4 = await offsetStore.GetCommittedOffsetAsync("g1", "t2", 0);

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
        
        var tasks = new List<Task>();
        for (int i = 0; i < 1000; i++)
        {
            int offsetToCommit = i;
            tasks.Add(Task.Run(async () => {
                await offsetStore.CommitOffsetAsync("g1", "t1", 0, offsetToCommit);
            }));
        }

        await Task.WhenAll(tasks);

        var finalValue = await offsetStore.GetCommittedOffsetAsync("g1", "t1", 0);
        finalValue.Should().Be(999);
    }
}
