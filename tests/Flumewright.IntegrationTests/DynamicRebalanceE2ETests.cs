using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Client;
using Flumewright.Protocol;
using Grpc.Core;
using Grpc.Net.Client;
using Xunit;

namespace Flumewright.IntegrationTests;

public sealed class DynamicRebalanceE2ETests : IClassFixture<BrokerAppFactory>
{
    private readonly BrokerAppFactory _factory;
    public DynamicRebalanceE2ETests(BrokerAppFactory factory) => _factory = factory;

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MembershipLifecycle_RedistributesOnJoinAndLeave()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.lifecycle." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };

        using var c1 = new FlumewrightGroupConsumer(address, groupId, "member-1");
        var c1Messages = new List<ReceivedMessage>();
        using var c1TaskCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
        var c1Task = ConsumeAsync(c1, topic, partitionCounts, strategy, c1Messages, c1TaskCts.Token);

        await EnsureReceivesFrom(publisher, topic, new[] { 0, 1, 2, 3 }, c1Messages, cts.Token);

        using var c2 = new FlumewrightGroupConsumer(address, groupId, "member-2");
        var c2Messages = new List<ReceivedMessage>();
        var c2Task = ConsumeAsync(c2, topic, partitionCounts, strategy, c2Messages, cts.Token);

        lock (c1Messages) c1Messages.Clear();
        await EnsureReceivesFrom(publisher, topic, new[] { 2, 3 }, c2Messages, cts.Token);
        await EnsureReceivesFrom(publisher, topic, new[] { 0, 1 }, c1Messages, cts.Token);

        // Clear messages before wait
        lock (c1Messages) c1Messages.Clear();
        await EnsureReceivesFrom(publisher, topic, new[] { 0, 1 }, c1Messages, cts.Token);

        await c1.LeaveGroupAsync(cts.Token);
        await c1TaskCts.CancelAsync();
        
        lock (c2Messages) c2Messages.Clear();
        await EnsureReceivesFrom(publisher, topic, new[] { 0, 1, 2, 3 }, c2Messages, cts.Token);

        await cts.CancelAsync();
        await Task.WhenAll(c1Task, c2Task);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task HandoverSafety_StartsFromCommittedOffset()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.handover." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };

        for (int i = 0; i < 5; i++)
        {
            await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes($"msg-{i}"), partitionKey: new byte[] { 0 }, ct: cts.Token);
        }

        var c1 = new FlumewrightGroupConsumer(address, groupId, "member-1");
        var iter1 = c1.SubscribeAsync(new[] { topic }, partitionCounts, strategy, FlumewrightOffsetReset.Earliest, TimeSpan.FromMilliseconds(500), cts.Token).GetAsyncEnumerator(cts.Token);
        
        var m1 = await iter1.MoveNextAsync();
        var m2 = await iter1.MoveNextAsync();
        
        iter1.Current.Offset.Should().Be(1);
        await c1.CommitOffsetAsync(topic, iter1.Current.Partition, 2, cts.Token);
        
        await c1.LeaveGroupAsync(cts.Token);
        c1.Dispose(); // stop member 1 entirely

        using var c2 = new FlumewrightGroupConsumer(address, groupId, "member-2");
        var iter2 = c2.SubscribeAsync(new[] { topic }, partitionCounts, strategy, FlumewrightOffsetReset.Earliest, TimeSpan.FromMilliseconds(500), cts.Token).GetAsyncEnumerator(cts.Token);
        
        var m3 = await iter2.MoveNextAsync();
        iter2.Current.Offset.Should().Be(2);
        
        await iter1.DisposeAsync();
        await iter2.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ZombieFencing_StaleGenerationCommit_Rejected()
    {
        var address = _factory.Address;
        var groupId = "cg-" + Guid.NewGuid();
        var topic = "it.rebalance.zombie." + Guid.NewGuid();

        using var channel = GrpcChannel.ForAddress(address);
        var client = new MessageBus.MessageBusClient(channel);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var join1 = await client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m1", Topics = { topic } }, cancellationToken: cts.Token);
        var gen1 = join1.Generation;
        await client.SyncGroupAsync(new SyncGroupRequest { GroupId = groupId, MemberId = "m1", Generation = gen1, Assignments = { new MemberAssignment { MemberId = "m1", Topic = topic, Partitions = { 0 } } } }, cancellationToken: cts.Token);

        var join2Task = client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m2", Topics = { topic } }, cancellationToken: cts.Token).ResponseAsync;
        
        while (!cts.IsCancellationRequested)
        {
            var ping = await client.HeartbeatAsync(new HeartbeatRequest { GroupId = groupId, MemberId = "m1", Generation = gen1 }, cancellationToken: cts.Token);
            if (ping.Code == GroupErrorCode.GroupFenced)
            {
                break;
            }
            await Task.Delay(100, cts.Token);
        }

        var commitRes = await client.CommitOffsetAsync(new CommitRequest { GroupId = groupId, Topic = topic, Partition = 0, Offset = 1, Generation = gen1 }, cancellationToken: cts.Token);
        commitRes.Ok.Should().BeFalse();
        commitRes.Code.Should().Be(GroupErrorCode.GroupFenced);
        
        var hbRes = await client.HeartbeatAsync(new HeartbeatRequest { GroupId = groupId, MemberId = "m1", Generation = gen1 }, cancellationToken: cts.Token);
        hbRes.Ok.Should().BeFalse();
        hbRes.Code.Should().Be(GroupErrorCode.GroupFenced);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Liveness_SlowHandler_NotEvicted()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.liveness." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };
        using var publisher = new FlumewrightPublisher(address);

        using var c1 = new FlumewrightGroupConsumer(address, groupId, "member-1");
        var iter = c1.SubscribeAsync(new[] { topic }, partitionCounts, strategy, FlumewrightOffsetReset.Earliest, TimeSpan.FromMilliseconds(500), cts.Token).GetAsyncEnumerator(cts.Token);
        
        await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes("msg-1"), partitionKey: new byte[] { 0 }, ct: cts.Token);
        await iter.MoveNextAsync();

        // Legitimate use of Task.Delay (integration-only, real timeout): testing actual wall-clock session timeout against real broker
        await Task.Delay(12000, cts.Token);

        await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes("msg-2"), partitionKey: new byte[] { 0 }, ct: cts.Token);
        await iter.MoveNextAsync(); 
        iter.Current.Payload.Should().Equal(System.Text.Encoding.UTF8.GetBytes("msg-2"));
        
        await iter.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Liveness_DeadMember_IsEvicted()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.dead." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        using var channel = GrpcChannel.ForAddress(address);
        var client = new MessageBus.MessageBusClient(channel);
        
        var join1 = await client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "dead", Topics = { topic } }, cancellationToken: cts.Token);
        var gen = join1.Generation;
        await client.SyncGroupAsync(new SyncGroupRequest { GroupId = groupId, MemberId = "dead", Generation = gen, Assignments = { new MemberAssignment { MemberId = "dead", Topic = topic, Partitions = { 0 } } } }, cancellationToken: cts.Token);
        
        // Legitimate use of Task.Delay (integration-only, real timeout): testing actual wall-clock session timeout against real broker
        await Task.Delay(12000, cts.Token);

        using var c2 = new FlumewrightGroupConsumer(address, groupId, "member-2");
        var strategy = new RangeAssignmentStrategy();
        var iter = c2.SubscribeAsync(new[] { topic }, new Dictionary<string, int> { { topic, 4 } }, strategy, FlumewrightOffsetReset.Earliest, TimeSpan.FromMilliseconds(500), cts.Token).GetAsyncEnumerator(cts.Token);
        
        using var publisher = new FlumewrightPublisher(address);
        await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes("msg"), partitionKey: new byte[] { 0 }, ct: cts.Token);
        
        var moved = await iter.MoveNextAsync();
        moved.Should().BeTrue();
        
        await iter.DisposeAsync();
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task HeartbeatFault_PropagatesToCaller_NotSwallowed()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.hbfault." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };
        using var publisher = new FlumewrightPublisher(address);
        await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes("msg"), partitionKey: new byte[] { 0 }, ct: cts.Token);

        var handler = new FaultInjectingHandler(new System.Net.Http.SocketsHttpHandler(), "Heartbeat", new RpcException(new Status(StatusCode.Unavailable, "Broker dead")), 1);
        using var channel = GrpcChannel.ForAddress(address, new GrpcChannelOptions { HttpHandler = handler });
        var client = new MessageBus.MessageBusClient(channel);
        
        var ctor = typeof(FlumewrightGroupConsumer).GetConstructors(System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance).First(c => c.GetParameters().Length == 3 && c.GetParameters()[0].ParameterType == typeof(MessageBus.MessageBusClient));
        using var c1 = (FlumewrightGroupConsumer)ctor.Invoke(new object[] { client, groupId, "member-1" });

        var iter = c1.SubscribeAsync(new[] { topic }, partitionCounts, strategy, FlumewrightOffsetReset.Earliest, TimeSpan.FromMilliseconds(500), cts.Token).GetAsyncEnumerator(cts.Token);
        
        Func<Task> act = async () =>
        {
            while (await iter.MoveNextAsync()) { }
        };
        
        try
        {
            var ex = await act.Should().ThrowAsync<Exception>();
            
            var actualEx = (Exception)ex.Subject.Single();
            if (actualEx is AggregateException agg) actualEx = agg.InnerException!;
            
            actualEx.Should().Match<Exception>(e => e is RpcException || (e.InnerException != null && e.InnerException is RpcException));
        }
        finally
        {
            await iter.DisposeAsync();
        }
    }

    private async Task ConsumeAsync(FlumewrightGroupConsumer consumer, string topic, IReadOnlyDictionary<string, int> partitionCounts, IAssignmentStrategy strategy, List<ReceivedMessage> targetList, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in consumer.SubscribeAsync(new[] { topic }, partitionCounts, strategy, FlumewrightOffsetReset.Latest, TimeSpan.FromMilliseconds(500), ct))
            {
                lock (targetList) targetList.Add(msg);
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task EnsureReceivesFrom(FlumewrightPublisher publisher, string topic, int[] expectedPartitions, List<ReceivedMessage> targetList, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            // Publish enough messages round-robin so that all partitions get at least one
            for (int i = 0; i < 8; i++)
            {
                await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes("msg"), partitionKey: Array.Empty<byte>(), ct: ct);
            }

            await Task.Delay(500, ct);
            lock (targetList)
            {
                var receivedPartitions = targetList.Select(m => m.Partition).Distinct().OrderBy(p => p).ToList();
                if (expectedPartitions.All(p => receivedPartitions.Contains(p)))
                {
                    return;
                }
            }
        }
        ct.ThrowIfCancellationRequested();
    }
}
