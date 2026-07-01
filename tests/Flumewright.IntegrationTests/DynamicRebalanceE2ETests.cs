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
    private static readonly int[] AllPartitions = new[] { 0, 1, 2, 3 };
    private static readonly int[] M1Partitions = new[] { 0, 1 };
    private static readonly int[] M2Partitions = new[] { 2, 3 };

    private readonly BrokerAppFactory _factory;
    public DynamicRebalanceE2ETests(BrokerAppFactory factory) => _factory = factory;

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
    [Fact]
    [Trait("Category", "Integration")]
    public async Task MembershipLifecycle_RedistributesOnJoinAndLeave()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.split." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var channel = GrpcChannel.ForAddress(address);
        var client = new MessageBus.MessageBusClient(channel);
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var partitionCountsPublished = new int[4];
        var targetPartitions = new HashSet<int> { 0, 1, 2, 3 };
        int keyIndex = 0;
        
        while (targetPartitions.Count > 0)
        {
            var ack = await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes($"init-{keyIndex}"), partitionKey: BitConverter.GetBytes(keyIndex), ct: cts.Token);
            targetPartitions.Remove(ack.Partition);
            partitionCountsPublished[ack.Partition]++;
            keyIndex++;
        }

        var join1 = await client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m1", Topics = { topic } }, cancellationToken: cts.Token);
        join1.Ok.Should().BeTrue(join1.Reason);
        var sync1 = await client.SyncGroupAsync(new SyncGroupRequest 
        { 
            GroupId = groupId, MemberId = "m1", Generation = join1.Generation, 
            Assignments = { new MemberAssignment { MemberId = "m1", Topic = topic, Partitions = { 0, 1, 2, 3 } } }
        }, cancellationToken: cts.Token);
        sync1.Ok.Should().BeTrue();

        using var sub1Call = client.Subscribe(new SubscribeRequest { Topic = topic, GroupId = groupId, Partitions = { 0, 1, 2, 3 }, Reset = OffsetReset.Earliest }, cancellationToken: cts.Token);
        var m1Partitions = new HashSet<int>();
        int m1InitialExpected = partitionCountsPublished.Sum();
        for (int i = 0; i < m1InitialExpected; i++)
        {
            var moved = await sub1Call.ResponseStream.MoveNext(cts.Token);
            moved.Should().BeTrue();
            m1Partitions.Add(sub1Call.ResponseStream.Current.Partition);
        }
        m1Partitions.Should().BeEquivalentTo(AllPartitions);

        var join2Task = client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m2", Topics = { topic } }, cancellationToken: cts.Token).ResponseAsync;
        
        while (true)
        {
            var hb = await client.HeartbeatAsync(new HeartbeatRequest { GroupId = groupId, MemberId = "m1", Generation = sync1.Generation }, cancellationToken: cts.Token);
            if (!hb.Ok)
            {
                (hb.Code == GroupErrorCode.GroupRebalanceInProgress || hb.Code == GroupErrorCode.GroupFenced).Should().BeTrue($"unexpected heartbeat error: {hb.Code}");
                break;
            }
            await Task.Delay(50, cts.Token);
        }

        var join1Task = client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m1", Topics = { topic } }, cancellationToken: cts.Token).ResponseAsync;
        
        var join1Res = await join1Task;
        var join2Res = await join2Task;
        join1Res.Ok.Should().BeTrue(join1Res.Reason);
        join2Res.Ok.Should().BeTrue(join2Res.Reason);
        (join1Res.IsLeader ^ join2Res.IsLeader).Should().BeTrue();
        join1Res.Generation.Should().Be(join2Res.Generation);
        var leaderJoin = join1Res.IsLeader ? join1Res : join2Res;
        
        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };
        var assignments = strategy.Assign(leaderJoin.Members, partitionCounts);

        var sync1Req = new SyncGroupRequest { GroupId = groupId, MemberId = "m1", Generation = leaderJoin.Generation };
        var sync2Req = new SyncGroupRequest { GroupId = groupId, MemberId = "m2", Generation = leaderJoin.Generation };
        if (join1Res.IsLeader) sync1Req.Assignments.AddRange(assignments);
        if (join2Res.IsLeader) sync2Req.Assignments.AddRange(assignments);

        var sync1Task = client.SyncGroupAsync(sync1Req, cancellationToken: cts.Token).ResponseAsync;
        var sync2Task = client.SyncGroupAsync(sync2Req, cancellationToken: cts.Token).ResponseAsync;
        
        var sync1Res = await sync1Task;
        var sync2Res = await sync2Task;
        sync1Res.Ok.Should().BeTrue(sync1Res.Reason);
        sync2Res.Ok.Should().BeTrue(sync2Res.Reason);
        
        var m1Assignments = sync1Res.Assignments.Single(a => a.Topic == topic).Partitions;
        var m2Assignments = sync2Res.Assignments.Single(a => a.Topic == topic).Partitions;
        m1Assignments.Should().BeEquivalentTo(M1Partitions);
        m2Assignments.Should().BeEquivalentTo(M2Partitions);

        var targetPartitions2 = new HashSet<int> { 0, 1, 2, 3 };
        while (targetPartitions2.Count > 0)
        {
            var ack = await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes($"post-{keyIndex}"), partitionKey: BitConverter.GetBytes(keyIndex), ct: cts.Token);
            targetPartitions2.Remove(ack.Partition);
            partitionCountsPublished[ack.Partition]++;
            keyIndex++;
        }

        using var sub1Final = client.Subscribe(new SubscribeRequest { Topic = topic, GroupId = groupId, Partitions = { m1Assignments }, Reset = OffsetReset.Earliest }, cancellationToken: cts.Token);
        using var sub2Final = client.Subscribe(new SubscribeRequest { Topic = topic, GroupId = groupId, Partitions = { m2Assignments }, Reset = OffsetReset.Earliest }, cancellationToken: cts.Token);

        var m1FinalPartitions = new HashSet<int>();
        int m1Expected = m1Assignments.Sum(p => partitionCountsPublished[p]);
        for (int i = 0; i < m1Expected; i++)
        {
            var moved = await sub1Final.ResponseStream.MoveNext(cts.Token);
            moved.Should().BeTrue();
            m1FinalPartitions.Add(sub1Final.ResponseStream.Current.Partition);
        }
        m1FinalPartitions.Should().BeEquivalentTo(M1Partitions);

        var m2FinalPartitions = new HashSet<int>();
        int m2Expected = m2Assignments.Sum(p => partitionCountsPublished[p]);
        for (int i = 0; i < m2Expected; i++)
        {
            var moved = await sub2Final.ResponseStream.MoveNext(cts.Token);
            moved.Should().BeTrue();
            m2FinalPartitions.Add(sub2Final.ResponseStream.Current.Partition);
        }
        m2FinalPartitions.Should().BeEquivalentTo(M2Partitions);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MembershipLifecycle_LeaderVanish_RemainingMemberTakesOver()
    {
        var address = _factory.Address;
        var topic = "it.rebalance.vanish." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        
        using var channel = GrpcChannel.ForAddress(address);
        var client = new MessageBus.MessageBusClient(channel);
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var partitionCountsPublished = new int[4];
        var targetPartitions = new HashSet<int> { 0, 1, 2, 3 };
        int keyIndex = 0;
        
        while (targetPartitions.Count > 0)
        {
            var ack = await publisher.PublishAckAsync(topic, System.Text.Encoding.UTF8.GetBytes($"init-{keyIndex}"), partitionKey: BitConverter.GetBytes(keyIndex), ct: cts.Token);
            targetPartitions.Remove(ack.Partition);
            partitionCountsPublished[ack.Partition]++;
            keyIndex++;
        }

        // Phase 1: m1 joins alone
        var join1 = await client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m1", Topics = { topic } }, cancellationToken: cts.Token);
        join1.Ok.Should().BeTrue(join1.Reason);
        var sync1 = await client.SyncGroupAsync(new SyncGroupRequest 
        { 
            GroupId = groupId, MemberId = "m1", Generation = join1.Generation, 
            Assignments = { new MemberAssignment { MemberId = "m1", Topic = topic, Partitions = { 0, 1, 2, 3 } } }
        }, cancellationToken: cts.Token);
        sync1.Ok.Should().BeTrue();

        // m2 joins, triggering rebalance. m1 discovers it via heartbeat.
        var join2Task = client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m2", Topics = { topic } }, cancellationToken: cts.Token).ResponseAsync;
        while (true)
        {
            var hb = await client.HeartbeatAsync(new HeartbeatRequest { GroupId = groupId, MemberId = "m1", Generation = sync1.Generation }, cancellationToken: cts.Token);
            if (!hb.Ok)
            {
                (hb.Code == GroupErrorCode.GroupRebalanceInProgress || hb.Code == GroupErrorCode.GroupFenced).Should().BeTrue($"unexpected heartbeat error: {hb.Code}");
                break;
            }
            await Task.Delay(50, cts.Token);
        }

        var join1Task = client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = "m1", Topics = { topic } }, cancellationToken: cts.Token).ResponseAsync;
        var join1Res = await join1Task;
        var join2Res = await join2Task;
        
        join1Res.Ok.Should().BeTrue(join1Res.Reason);
        join2Res.Ok.Should().BeTrue(join2Res.Reason);
        (join1Res.IsLeader ^ join2Res.IsLeader).Should().BeTrue();
        
        var leaderJoin = join1Res.IsLeader ? join1Res : join2Res;
        var followerId = join1Res.IsLeader ? "m2" : "m1";
        
        var strategy = new RangeAssignmentStrategy();
        var partitionCounts = new Dictionary<string, int> { { topic, 4 } };
        var assignments = strategy.Assign(leaderJoin.Members, partitionCounts);

        var sync1Req = new SyncGroupRequest { GroupId = groupId, MemberId = "m1", Generation = leaderJoin.Generation };
        var sync2Req = new SyncGroupRequest { GroupId = groupId, MemberId = "m2", Generation = leaderJoin.Generation };
        if (join1Res.IsLeader) sync1Req.Assignments.AddRange(assignments);
        if (join2Res.IsLeader) sync2Req.Assignments.AddRange(assignments);

        var sync1TaskInner = client.SyncGroupAsync(sync1Req, cancellationToken: cts.Token).ResponseAsync;
        var sync2TaskInner = client.SyncGroupAsync(sync2Req, cancellationToken: cts.Token).ResponseAsync;
        
        var sync1Res = await sync1TaskInner;
        var sync2Res = await sync2TaskInner;
        sync1Res.Ok.Should().BeTrue(sync1Res.Reason);
        sync2Res.Ok.Should().BeTrue(sync2Res.Reason);

        // Phase 2: Leader vanishes (no more heartbeats for leaderId). Follower continues to heartbeat.
        // The sweeper evicts the leader after 1s timeout.
        while (true)
        {
            var hb = await client.HeartbeatAsync(new HeartbeatRequest { GroupId = groupId, MemberId = followerId, Generation = leaderJoin.Generation }, cancellationToken: cts.Token);
            if (!hb.Ok)
            {
                (hb.Code == GroupErrorCode.GroupRebalanceInProgress || hb.Code == GroupErrorCode.GroupFenced).Should().BeTrue($"unexpected heartbeat error: {hb.Code}");
                break;
            }
            await Task.Delay(50, cts.Token);
        }

        // Phase 3: Follower rejoins and becomes the new leader.
        var followerRejoin = await client.JoinGroupAsync(new JoinGroupRequest { GroupId = groupId, MemberId = followerId, Topics = { topic } }, cancellationToken: cts.Token);
        followerRejoin.Ok.Should().BeTrue(followerRejoin.Reason);
        followerRejoin.IsLeader.Should().BeTrue("remaining member should be elected leader after previous leader vanished");
        followerRejoin.Members.Count.Should().Be(1, "only the follower should remain in the group");

        var rejoinAssignments = strategy.Assign(followerRejoin.Members, partitionCounts);
        var syncFollowerRejoin = await client.SyncGroupAsync(new SyncGroupRequest 
        { 
            GroupId = groupId, MemberId = followerId, Generation = followerRejoin.Generation, 
            Assignments = { rejoinAssignments }
        }, cancellationToken: cts.Token);
        
        syncFollowerRejoin.Ok.Should().BeTrue(syncFollowerRejoin.Reason);
        
        // Assert new leader now owns all partitions
        var finalAssignments = syncFollowerRejoin.Assignments.Single(a => a.Topic == topic).Partitions;
        finalAssignments.Should().BeEquivalentTo(AllPartitions);

        // Verify message flow for all partitions via the new leader
        using var subFinal = client.Subscribe(new SubscribeRequest { Topic = topic, GroupId = groupId, Partitions = { finalAssignments }, Reset = OffsetReset.Earliest }, cancellationToken: cts.Token);
        
        var finalConsumed = new HashSet<int>();
        int finalExpected = finalAssignments.Sum(p => partitionCountsPublished[p]);
        for (int i = 0; i < finalExpected; i++)
        {
            var moved = await subFinal.ResponseStream.MoveNext(cts.Token);
            moved.Should().BeTrue();
            finalConsumed.Add(subFinal.ResponseStream.Current.Partition);
        }
        finalConsumed.Should().BeEquivalentTo(AllPartitions);
    }
}
