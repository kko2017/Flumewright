using FluentAssertions;
using Flumewright.Client;
using Flumewright.Protocol;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Flumewright.UnitTests.Client;

public class FlumewrightGroupConsumerTests
{
    [Fact]
    public async Task CombineStaticAndDynamic_ThrowsInvalidOperationException()
    {
        var fakeClient = new FakeMessageBusClient();
        var consumer = new FlumewrightGroupConsumer(fakeClient, "group-id", "member-id");

        // Use static assign first
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var assignEnumerable = consumer.AssignAsync("topic", new[] { 0, 1 }, ct: cts.Token);
        await assignEnumerable.GetAsyncEnumerator().MoveNextAsync();

        // Now try dynamic subscribe
        var subscribeAction = async () =>
        {
            var dynamicEnum = consumer.SubscribeAsync(
                new[] { "topic" },
                new Dictionary<string, int>(),
                new RangeAssignmentStrategy());
            await dynamicEnum.GetAsyncEnumerator().MoveNextAsync();
        };

        await subscribeAction.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be mixed*");
    }

    [Fact]
    public async Task CombineDynamicAndStatic_ThrowsInvalidOperationException()
    {
        var fakeClient = new FakeMessageBusClient();
        var consumer = new FlumewrightGroupConsumer(fakeClient, "group-id", "member-id");

        // Use dynamic subscribe first
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var subscribeEnumerable = consumer.SubscribeAsync(
            new[] { "topic" },
            new Dictionary<string, int>(),
            new RangeAssignmentStrategy(),
            ct: cts.Token);
        await subscribeEnumerable.GetAsyncEnumerator().MoveNextAsync();

        // Now try static assign
        var assignAction = async () =>
        {
            var assignEnum = consumer.AssignAsync("topic", new[] { 0, 1 });
            await assignEnum.GetAsyncEnumerator().MoveNextAsync();
        };

        await assignAction.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*cannot be mixed*");
    }

    private class FakeMessageBusClient : MessageBus.MessageBusClient
    {
        public Func<HeartbeatRequest, Task<HeartbeatResponse>>? OnHeartbeat { get; set; }
        public Func<JoinGroupRequest, Task<JoinGroupResponse>>? OnJoinGroup { get; set; }
        public Func<SyncGroupRequest, Task<SyncGroupResponse>>? OnSyncGroup { get; set; }

        public FakeMessageBusClient() : base() { }

        public override Grpc.Core.AsyncUnaryCall<JoinGroupResponse> JoinGroupAsync(JoinGroupRequest request, Grpc.Core.CallOptions options)
        {
            var res = OnJoinGroup?.Invoke(request) ?? Task.FromResult(new JoinGroupResponse { Ok = true, Generation = 1 });
            return new Grpc.Core.AsyncUnaryCall<JoinGroupResponse>(res, Task.FromResult(new Grpc.Core.Metadata()), () => Grpc.Core.Status.DefaultSuccess, () => new Grpc.Core.Metadata(), () => { });
        }

        public override Grpc.Core.AsyncUnaryCall<SyncGroupResponse> SyncGroupAsync(SyncGroupRequest request, Grpc.Core.CallOptions options)
        {
            var res = OnSyncGroup?.Invoke(request) ?? Task.FromResult(new SyncGroupResponse { Ok = true, Generation = 1 });
            return new Grpc.Core.AsyncUnaryCall<SyncGroupResponse>(res, Task.FromResult(new Grpc.Core.Metadata()), () => Grpc.Core.Status.DefaultSuccess, () => new Grpc.Core.Metadata(), () => { });
        }

        public override Grpc.Core.AsyncUnaryCall<HeartbeatResponse> HeartbeatAsync(HeartbeatRequest request, Grpc.Core.CallOptions options)
        {
            var res = OnHeartbeat?.Invoke(request) ?? Task.FromResult(new HeartbeatResponse { Ok = true });
            return new Grpc.Core.AsyncUnaryCall<HeartbeatResponse>(res, Task.FromResult(new Grpc.Core.Metadata()), () => Grpc.Core.Status.DefaultSuccess, () => new Grpc.Core.Metadata(), () => { });
        }

        private class FakeStreamReader : Grpc.Core.IAsyncStreamReader<DeliverEnvelope>
        {
            public DeliverEnvelope Current => default!;
            public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
        }

        public override Grpc.Core.AsyncServerStreamingCall<DeliverEnvelope> Subscribe(SubscribeRequest request, Grpc.Core.CallOptions options)
        {
            return new Grpc.Core.AsyncServerStreamingCall<DeliverEnvelope>(
                new FakeStreamReader(),
                Task.FromResult(new Grpc.Core.Metadata()),
                () => Grpc.Core.Status.DefaultSuccess,
                () => new Grpc.Core.Metadata(),
                () => { });
        }
    }

    [Fact]
    public async Task HeartbeatLoop_PropagatesFault_DoesNotSwallow()
    {
        var fakeClient = new FakeMessageBusClient();
        var consumer = new FlumewrightGroupConsumer(fakeClient, "group-id", "member-id");

        fakeClient.OnHeartbeat = req => throw new Exception("Simulated heartbeat fault");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var enumerable = consumer.SubscribeAsync(
            new[] { "topic" },
            new Dictionary<string, int>(),
            new RangeAssignmentStrategy(),
            heartbeatInterval: TimeSpan.FromMilliseconds(50),
            ct: cts.Token);

        var moveNextAction = async () =>
        {
            await using var enumerator = enumerable.GetAsyncEnumerator(cts.Token);
            await enumerator.MoveNextAsync();
        };

        var ex = await moveNextAction.Should().ThrowAsync<Exception>();
        ex.WithMessage("Simulated heartbeat fault");
    }

    [Fact]
    public async Task HeartbeatLoop_StopsCleanlyOnCancellation()
    {
        var fakeClient = new FakeMessageBusClient();
        var consumer = new FlumewrightGroupConsumer(fakeClient, "group-id", "member-id");

        var heartbeatCount = 0;
        var tcs = new TaskCompletionSource();
        fakeClient.OnHeartbeat = req =>
        {
            if (Interlocked.Increment(ref heartbeatCount) >= 2)
            {
                tcs.TrySetResult();
            }
            return Task.FromResult(new HeartbeatResponse { Ok = true });
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var enumerable = consumer.SubscribeAsync(
            new[] { "topic" },
            new Dictionary<string, int>(),
            new RangeAssignmentStrategy(),
            heartbeatInterval: TimeSpan.FromMilliseconds(50),
            ct: cts.Token);

        var consumeTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in enumerable.WithCancellation(cts.Token)) { }
            }
            catch (OperationCanceledException) { }
        });

        // Wait for at least 2 heartbeats (at 3 seconds each)
        await Task.WhenAny(tcs.Task, Task.Delay(10000));
        
        await cts.CancelAsync();
        
        await consumeTask; // Should complete cleanly without exception
        heartbeatCount.Should().BeGreaterThanOrEqualTo(2);
    }
}
