using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Client;
using Flumewright.Client.Resilience;
using Grpc.Core;
using Xunit;

namespace Flumewright.IntegrationTests;

public sealed class RedeliveryDlqE2ETests : IClassFixture<BrokerAppFactory>
{
    private readonly BrokerAppFactory _factory;
    public RedeliveryDlqE2ETests(BrokerAppFactory factory) => _factory = factory;

    private Task RunPumpAsync(Func<Task> action, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            try
            {
                await action();
            }
#pragma warning disable CA1031
            catch (OperationCanceledException) { }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled) { }
            catch (Exception ex)
            {
                Console.WriteLine($"Pump failed: {ex}");
                throw;
            }
#pragma warning restore CA1031
        }, ct);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task TransientFailure_GoesToRetryAndAdvancesOffset()
    {
        var address = _factory.Address;
        var topic = "it.dlq.transient." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = Encoding.UTF8.GetBytes("key1");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var ack = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("fail"), partitionKey: key, ct: cts.Token);
        var partition = ack.Partition;

        await publisher.PublishAckAsync($"{topic}.retry", Encoding.UTF8.GetBytes("dummy"), ct: cts.Token);

        using var subscriber = new FlumewrightSubscriber(address);
        var policy = new DefaultRetryPolicy(3);
        var retryingConsumer = new RetryingConsumer(subscriber, publisher, policy);

        var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
        var syncReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pump = RunPumpAsync(async () =>
        {
            await retryingConsumer.ConsumeGroupAsync(topic, groupId, new[] { partition }, async (msg, ct) =>
            {
                var payload = Encoding.UTF8.GetString(msg.Payload);
                if (payload == "fail") throw new Exception("Transient failure");
                if (payload == "sync")
                {
                    syncReceived.TrySetResult(true);
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                }
            }, FlumewrightOffsetReset.Earliest, pumpCts.Token);
        }, pumpCts.Token);

        using var retrySub = new FlumewrightSubscriber(address);
        var retryReceived = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var retryPump = RunPumpAsync(async () =>
        {
            await foreach (var msg in retrySub.SubscribeGroupAsync($"{topic}.retry", "retry-cg", new[] { 0, 1, 2, 3 }, FlumewrightOffsetReset.Earliest, cts.Token))
            {
                if (Encoding.UTF8.GetString(msg.Payload) == "dummy") continue;
                retryReceived.TrySetResult(msg);
                break;
            }
        }, cts.Token);

        var retryMsg = await retryReceived.Task.WaitAsync(cts.Token);

        retryMsg.Headers.Should().ContainKey("x-attempts").WhoseValue.Should().Be("1");
        retryMsg.Headers.Should().ContainKey("x-original-topic").WhoseValue.Should().Be(topic);
        retryMsg.Headers.Should().ContainKey("x-original-partition").WhoseValue.Should().Be(partition.ToString());
        retryMsg.Headers.Should().ContainKey("x-original-offset").WhoseValue.Should().Be(ack.Offset.ToString());

        await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("sync"), partitionKey: key, ct: cts.Token);
        await syncReceived.Task.WaitAsync(cts.Token);

        await pumpCts.CancelAsync();
        await pump;

        var ack2 = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("healthy"), partitionKey: key, ct: cts.Token);

        using var verifySub = new FlumewrightSubscriber(address);
        var verifyReceived = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var verifyPump = RunPumpAsync(async () =>
        {
            await foreach (var msg in verifySub.SubscribeGroupAsync(topic, groupId, new[] { partition }, FlumewrightOffsetReset.Earliest, cts.Token))
            {
                verifyReceived.TrySetResult(msg);
                break;
            }
        }, cts.Token);

        var verifyMsg = await verifyReceived.Task.WaitAsync(cts.Token);

        // We expect to read "sync" because its offset was never committed (the pump was cancelled).
        Encoding.UTF8.GetString(verifyMsg.Payload).Should().Be("sync");
        verifyMsg.Offset.Should().BeGreaterThan(ack.Offset);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CyclesThroughRetryAndLandsInDlq()
    {
        var address = _factory.Address;
        var topic = "it.dlq.cycle." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = Encoding.UTF8.GetBytes("key1");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));

        var ack = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("fail"), partitionKey: key, ct: cts.Token);
        var partition = ack.Partition;

        await publisher.PublishAckAsync($"{topic}.retry", Encoding.UTF8.GetBytes("dummy"), ct: cts.Token);
        await publisher.PublishAckAsync($"{topic}.dlq", Encoding.UTF8.GetBytes("dummy"), ct: cts.Token);

        using var subscriber = new FlumewrightSubscriber(address);
        var policy = new DefaultRetryPolicy(3);
        var retryingConsumer = new RetryingConsumer(subscriber, publisher, policy);

        var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
        var pump = RunPumpAsync(async () =>
        {
            await retryingConsumer.ConsumeWithRetriesAsync(
                topic,
                groupId,
                new[] { partition },
                new[] { 0, 1, 2, 3 },
                (msg, ct) =>
                {
                    if (Encoding.UTF8.GetString(msg.Payload) == "dummy") return Task.CompletedTask;
                    throw new Exception("Always fails");
                },
                FlumewrightOffsetReset.Earliest,
                pumpCts.Token);
        }, pumpCts.Token);

        using var dlqSub = new FlumewrightSubscriber(address);
        var dlqReceived = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var dlqPump = RunPumpAsync(async () =>
        {
            await foreach (var msg in dlqSub.SubscribeGroupAsync($"{topic}.dlq", "dlq-cg", new[] { 0, 1, 2, 3 }, FlumewrightOffsetReset.Earliest, cts.Token))
            {
                if (Encoding.UTF8.GetString(msg.Payload) == "dummy") continue;
                dlqReceived.TrySetResult(msg);
                break;
            }
        }, cts.Token);

        var dlqMsg = await dlqReceived.Task.WaitAsync(cts.Token);

        await pumpCts.CancelAsync();
        await pump;

        dlqMsg.Headers.Should().ContainKey("x-attempts").WhoseValue.Should().Be("4");
        dlqMsg.Headers.Should().ContainKey("x-original-topic").WhoseValue.Should().Be(topic);
        dlqMsg.Headers.Should().ContainKey("x-original-partition").WhoseValue.Should().Be(partition.ToString());
        dlqMsg.Headers.Should().ContainKey("x-original-offset").WhoseValue.Should().Be(ack.Offset.ToString());
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PoisonMessage_GoesStraightToDlq()
    {
        var address = _factory.Address;
        var topic = "it.dlq.poison." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = Encoding.UTF8.GetBytes("key1");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var ack = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("poison"), partitionKey: key, ct: cts.Token);
        var partition = ack.Partition;

        await publisher.PublishAckAsync($"{topic}.dlq", Encoding.UTF8.GetBytes("dummy"), ct: cts.Token);

        using var subscriber = new FlumewrightSubscriber(address);
        var policy = new DefaultRetryPolicy(3);
        var retryingConsumer = new RetryingConsumer(subscriber, publisher, policy);

        var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
        var pump = RunPumpAsync(async () =>
        {
            await retryingConsumer.ConsumeGroupAsync(topic, groupId, new[] { partition }, (msg, ct) =>
            {
                throw new PoisonMessageException("Poison pill");
            }, FlumewrightOffsetReset.Earliest, pumpCts.Token);
        }, pumpCts.Token);

        using var dlqSub = new FlumewrightSubscriber(address);
        var dlqReceived = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var dlqPump = RunPumpAsync(async () =>
        {
            await foreach (var msg in dlqSub.SubscribeGroupAsync($"{topic}.dlq", "dlq-cg", new[] { 0, 1, 2, 3 }, FlumewrightOffsetReset.Earliest, cts.Token))
            {
                if (Encoding.UTF8.GetString(msg.Payload) == "dummy") continue;
                dlqReceived.TrySetResult(msg);
                break;
            }
        }, cts.Token);

        var dlqMsg = await dlqReceived.Task.WaitAsync(cts.Token);

        await pumpCts.CancelAsync();
        await pump;

        dlqMsg.Headers.Should().ContainKey("x-failure-reason").WhoseValue.Should().Be("PoisonMessageException");
        dlqMsg.Headers.Should().ContainKey("x-attempts").WhoseValue.Should().Be("1");
        dlqMsg.Headers.Should().ContainKey("x-original-topic").WhoseValue.Should().Be(topic);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task HeadOfLineUnblock_HealthyMessageAfterFailureIsConsumed()
    {
        var address = _factory.Address;
        var topic = "it.dlq.hol." + Guid.NewGuid();
        var groupId = "cg-" + Guid.NewGuid();
        var key = Encoding.UTF8.GetBytes("key1");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        await publisher.PublishAckAsync($"{topic}.retry", Encoding.UTF8.GetBytes("dummy"), ct: cts.Token);

        var ack1 = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("fail"), partitionKey: key, ct: cts.Token);
        var partition = ack1.Partition;
        var ack2 = await publisher.PublishAckAsync(topic, Encoding.UTF8.GetBytes("healthy"), partitionKey: key, ct: cts.Token);

        using var subscriber = new FlumewrightSubscriber(address);
        var policy = new DefaultRetryPolicy(3);
        var retryingConsumer = new RetryingConsumer(subscriber, publisher, policy);

        var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
        var healthyReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = RunPumpAsync(async () =>
        {
            await retryingConsumer.ConsumeGroupAsync(topic, groupId, new[] { partition }, (msg, ct) =>
            {
                var payload = Encoding.UTF8.GetString(msg.Payload);
                if (payload == "fail") throw new Exception("fail");
                if (payload == "healthy") healthyReceived.TrySetResult(true);
                return Task.CompletedTask;
            }, FlumewrightOffsetReset.Earliest, pumpCts.Token);
        }, pumpCts.Token);

        await healthyReceived.Task.WaitAsync(cts.Token);
        await pumpCts.CancelAsync();
        await pump;
    }
}
