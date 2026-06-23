using FluentAssertions;
using Flumewright.Client;
using Xunit;

namespace Flumewright.IntegrationTests;

public sealed class ConsumerGroupE2ETests : IClassFixture<BrokerAppFactory>
{
    private readonly BrokerAppFactory _factory;
    public ConsumerGroupE2ETests(BrokerAppFactory factory) => _factory = factory;

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CommitAndResume_StartsFromCommittedOffset_DoesNotRedeliver()
    {
        var address = _factory.Address;
        var topic = "it.cg.resume." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = System.Text.Encoding.UTF8.GetBytes("cg-key");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        int partition = -1;
        for (int i = 0; i < 5; i++)
        {
            var payload = System.Text.Encoding.UTF8.GetBytes($"msg-{i}");
            var ack = await publisher.PublishAckAsync(topic, payload, partitionKey: key, ct: cts.Token);
            partition = ack.Partition;
        }

        using var subscriber1 = new FlumewrightSubscriber(address);
        
        var receivedFirst = new List<ReceivedMessage>();
        await foreach (var msg in subscriber1.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedFirst.Add(msg);
            if (receivedFirst.Count == 3) break;
        }

        receivedFirst.Count.Should().Be(3);
        receivedFirst[0].Offset.Should().Be(0);
        receivedFirst[1].Offset.Should().Be(1);
        receivedFirst[2].Offset.Should().Be(2);

        await subscriber1.CommitOffsetAsync(groupId, topic, partition, 3, cts.Token);

        using var subscriber2 = new FlumewrightSubscriber(address);
        var receivedSecond = new List<ReceivedMessage>();
        await foreach (var msg in subscriber2.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedSecond.Add(msg);
            if (receivedSecond.Count == 2) break;
        }

        receivedSecond.Count.Should().Be(2);
        receivedSecond[0].Offset.Should().Be(3);
        receivedSecond[1].Offset.Should().Be(4);
    }
    
    [Fact]
    [Trait("Category", "Integration")]
    public async Task CrashAndResume_RedeliversUncommittedWindow()
    {
        var address = _factory.Address;
        var topic = "it.cg.crash." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = System.Text.Encoding.UTF8.GetBytes("cg-key");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        int partition = -1;
        for (int i = 0; i < 5; i++)
        {
            var payload = System.Text.Encoding.UTF8.GetBytes($"msg-{i}");
            var ack = await publisher.PublishAckAsync(topic, payload, partitionKey: key, ct: cts.Token);
            partition = ack.Partition;
        }

        using var subscriber1 = new FlumewrightSubscriber(address);
        
        var receivedFirst = new List<ReceivedMessage>();
        await foreach (var msg in subscriber1.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedFirst.Add(msg);
            if (receivedFirst.Count == 4) break;
        }

        receivedFirst.Count.Should().Be(4);

        await subscriber1.CommitOffsetAsync(groupId, topic, partition, 2, cts.Token);

        using var subscriber2 = new FlumewrightSubscriber(address);
        var receivedSecond = new List<ReceivedMessage>();
        await foreach (var msg in subscriber2.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedSecond.Add(msg);
            if (receivedSecond.Count == 3) break;
        }

        receivedSecond.Count.Should().Be(3);
        receivedSecond[0].Offset.Should().Be(2);
        receivedSecond[1].Offset.Should().Be(3);
        receivedSecond[2].Offset.Should().Be(4);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BackwardsCommit_IsRejected_AndDoesNotChangeCommittedOffset()
    {
        var address = _factory.Address;
        var topic = "it.cg.backwards." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = System.Text.Encoding.UTF8.GetBytes("cg-key");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        int partition = -1;
        for (int i = 0; i < 5; i++)
        {
            var payload = System.Text.Encoding.UTF8.GetBytes($"msg-{i}");
            var ack = await publisher.PublishAckAsync(topic, payload, partitionKey: key, ct: cts.Token);
            partition = ack.Partition;
        }

        using var subscriber1 = new FlumewrightSubscriber(address);

        await subscriber1.CommitOffsetAsync(groupId, topic, partition, 3, cts.Token);

        Func<Task> act = async () => await subscriber1.CommitOffsetAsync(groupId, topic, partition, 2, cts.Token);
        await act.Should().ThrowAsync<CommitRejectedException>();

        using var subscriber2 = new FlumewrightSubscriber(address);
        var receivedSecond = new List<ReceivedMessage>();
        await foreach (var msg in subscriber2.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedSecond.Add(msg);
            if (receivedSecond.Count == 2) break;
        }

        receivedSecond.Count.Should().Be(2);
        receivedSecond[0].Offset.Should().Be(3);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task FirstSubscribe_ResetPolicy_EarliestAndLatest()
    {
        var address = _factory.Address;
        var topic = "it.cg.reset." + Guid.NewGuid();
        var groupIdEarliest = "cg-earliest";
        var groupIdLatest = "cg-latest";
        var key = System.Text.Encoding.UTF8.GetBytes("cg-key");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        int partition = -1;
        for (int i = 0; i < 3; i++)
        {
            var payload = System.Text.Encoding.UTF8.GetBytes($"msg-{i}");
            var ack = await publisher.PublishAckAsync(topic, payload, partitionKey: key, ct: cts.Token);
            partition = ack.Partition;
        }

        using var subEarliest = new FlumewrightSubscriber(address);
        var receivedEarliest = new List<ReceivedMessage>();
        await foreach (var msg in subEarliest.SubscribeGroupAsync(topic, groupIdEarliest, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
        {
            receivedEarliest.Add(msg);
            if (receivedEarliest.Count == 3) break;
        }

        receivedEarliest.Count.Should().Be(3);
        receivedEarliest[0].Offset.Should().Be(0);

        using var subLatest = new FlumewrightSubscriber(address);
        
        var syncReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var receivedLatest = new List<ReceivedMessage>();
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in subLatest.SubscribeGroupAsync(topic, groupIdLatest, new[] { partition }, FlumewrightOffsetReset.Latest, cts.Token))
                {
                    if (System.Text.Encoding.UTF8.GetString(msg.Payload) == "sync")
                    {
                        syncReceived.TrySetResult(true);
                    }
                    else
                    {
                        lock (receivedLatest)
                        {
                            receivedLatest.Add(msg);
                            tcs.TrySetResult(true);
                            break;
                        }
                    }
                }
            }
#pragma warning disable CA1031
            catch (Exception ex)
            {
                syncReceived.TrySetException(ex);
                tcs.TrySetException(ex);
            }
#pragma warning restore CA1031
        }, cts.Token);

        while (!syncReceived.Task.IsCompleted && !cts.IsCancellationRequested)
        {
            await publisher.PublishAsync(topic, System.Text.Encoding.UTF8.GetBytes("sync"), partitionKey: key, ct: cts.Token);
            var done = await Task.WhenAny(syncReceived.Task, Task.Delay(200, cts.Token));
            if (done == syncReceived.Task) break;
        }
        await syncReceived.Task.WaitAsync(cts.Token);

        var payloadLatest = System.Text.Encoding.UTF8.GetBytes("msg-latest");
        var ackLatest = await publisher.PublishAckAsync(topic, payloadLatest, partitionKey: key, ct: cts.Token);

        await tcs.Task.WaitAsync(cts.Token);

        lock (receivedLatest)
        {
            receivedLatest.Count.Should().Be(1);
            receivedLatest[0].Payload.Should().Equal(payloadLatest);
            receivedLatest[0].Offset.Should().Be(ackLatest.Offset);
            receivedLatest[0].Offset.Should().BeGreaterThan(2);
        }
    }
}
