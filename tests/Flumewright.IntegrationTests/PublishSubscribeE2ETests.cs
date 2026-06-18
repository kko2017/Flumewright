using FluentAssertions;
using Flumewright.Client;
using Flumewright.Protocol;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.IntegrationTests;

public sealed class PublishSubscribeE2ETests : IClassFixture<BrokerAppFactory>
{
    private readonly BrokerAppFactory _factory;
    public PublishSubscribeE2ETests(BrokerAppFactory factory) => _factory = factory;

    [Fact]
    [Trait("Category", "Integration")]
    public async Task SingleMessage_FlowsFromPublisherToSubscriber()
    {
        // (If the 4b switch was needed, once before creating the first channel here — see 08 Step 4 findings)
         //AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        var address = _factory.Address;    // http://127.0.0.1:{port}
        const string topic = "it.demo";
        var payload = System.Text.Encoding.UTF8.GetBytes("hello-e2e");

        using var subscriber = new FlumewrightSubscriber(address);
        using var publisher = new FlumewrightPublisher(address);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Mitigate Trap ②: Start the subscription in the background first to establish the stream.
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = Task.Run(async () =>
        {
            await foreach (var msg in subscriber.SubscribeAsync(topic, cts.Token))
            {
                received.TrySetResult(msg);
                break; // 1 message is enough
            }
        }, cts.Token);

        // Reliably ensure time for the subscription stream to register with the broker:
        // (Simply & robustly: publish with short retries — since it is LATEST, a too-early first publish might be missed,
        //  so republish a few times until it arrives. Avoid using Delay alone; block flakiness with a "re-publish + timeout wait" combo.)
        long lastOffset = -1;
        while(!received.Task.IsCompleted && !cts.IsCancellationRequested)
        {
            lastOffset = await publisher.PublishAsync(topic, payload, ct: cts.Token);
            var done = await Task.WhenAny(received.Task, Task.Delay(200, cts.Token));
            if (done == received.Task) break;
        }

        var got = await received.Task.WaitAsync(cts.Token);

        got.Topic.Should().Be(topic);
        got.Payload.Should().Equal(payload);
        lastOffset.Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task AppendAndRoundTrip_Succeeds()
    {
        var address = _factory.Address;
        var topic = "it.roundtrip." + Guid.NewGuid();
        var key = System.Text.Encoding.UTF8.GetBytes("my-partition-key");
        var payload = System.Text.Encoding.UTF8.GetBytes("payload-roundtrip");

        using var subscriber = new FlumewrightSubscriber(address);
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in subscriber.SubscribeAsync(topic, cts.Token))
                {
                    received.TrySetResult(msg);
                    break;
                }
            }
            catch (Exception ex)
            {
                received.TrySetException(ex);
            }
        }, cts.Token);

        PublishAck? ack = null;
        while (!received.Task.IsCompleted && !cts.IsCancellationRequested)
        {
            ack = await publisher.PublishAckAsync(topic, payload, partitionKey: key, ct: cts.Token);
            var done = await Task.WhenAny(received.Task, Task.Delay(200, cts.Token));
            if (done == received.Task) break;
        }

        var got = await received.Task.WaitAsync(cts.Token);

        ack.Should().NotBeNull();
        ack!.Accepted.Should().BeTrue();
        ack.Partition.Should().BeInRange(0, 3); // Default partition count configured is 4
        ack.Offset.Should().BeGreaterThanOrEqualTo(0);

        got.Topic.Should().Be(topic);
        got.Payload.Should().Equal(payload);
        got.Partition.Should().Be(ack.Partition);
        got.Offset.Should().Be(ack.Offset);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task SameKey_SamePartitionAndOrder_Succeeds()
    {
        var address = _factory.Address;
        var topic = "it.ordering." + Guid.NewGuid();
        var key = System.Text.Encoding.UTF8.GetBytes("same-key");
        var syncPayload = System.Text.Encoding.UTF8.GetBytes("sync");
        var payload1 = System.Text.Encoding.UTF8.GetBytes("msg-1");
        var payload2 = System.Text.Encoding.UTF8.GetBytes("msg-2");

        using var subscriber = new FlumewrightSubscriber(address);
        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var syncReceived = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = new List<ReceivedMessage>();
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = Task.Run(async () =>
        {
            try
            {
                await foreach (var msg in subscriber.SubscribeAsync(topic, cts.Token))
                {
                    if (System.Text.Encoding.UTF8.GetString(msg.Payload) == "sync")
                    {
                        syncReceived.TrySetResult(true);
                    }
                    else
                    {
                        lock (received)
                        {
                            received.Add(msg);
                            if (received.Count == 2)
                            {
                                tcs.TrySetResult(true);
                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                syncReceived.TrySetException(ex);
                tcs.TrySetException(ex);
            }
        }, cts.Token);

        // 1. Establish subscription via sync message
        while (!syncReceived.Task.IsCompleted && !cts.IsCancellationRequested)
        {
            await publisher.PublishAsync(topic, syncPayload, ct: cts.Token);
            var done = await Task.WhenAny(syncReceived.Task, Task.Delay(200, cts.Token));
            if (done == syncReceived.Task) break;
        }
        await syncReceived.Task.WaitAsync(cts.Token);

        // 2. Publish two messages with the same key
        var ack1 = await publisher.PublishAckAsync(topic, payload1, partitionKey: key, ct: cts.Token);
        var ack2 = await publisher.PublishAckAsync(topic, payload2, partitionKey: key, ct: cts.Token);

        // 3. Wait for both to be received
        await tcs.Task.WaitAsync(cts.Token);

        ack1.Accepted.Should().BeTrue();
        ack2.Accepted.Should().BeTrue();

        // Same key -> same partition
        ack1.Partition.Should().Be(ack2.Partition);

        // Strictly increasing offsets
        ack2.Offset.Should().BeGreaterThan(ack1.Offset);

        // Delivered in order
        lock (received)
        {
            received.Count.Should().Be(2);
            received[0].Payload.Should().Equal(payload1);
            received[0].Partition.Should().Be(ack1.Partition);
            received[0].Offset.Should().Be(ack1.Offset);

            received[1].Payload.Should().Equal(payload2);
            received[1].Partition.Should().Be(ack2.Partition);
            received[1].Offset.Should().Be(ack2.Offset);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task RetainedRead_Succeeds()
    {
        var address = _factory.Address;
        var topic = "it.retained." + Guid.NewGuid();
        var payload1 = System.Text.Encoding.UTF8.GetBytes("retained-1");
        var payload2 = System.Text.Encoding.UTF8.GetBytes("retained-2");
        var payload3 = System.Text.Encoding.UTF8.GetBytes("retained-3");

        using var publisher = new FlumewrightPublisher(address);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Publish 3 messages before any subscription is active
        var ack1 = await publisher.PublishAckAsync(topic, payload1, ct: cts.Token);
        var ack2 = await publisher.PublishAckAsync(topic, payload2, ct: cts.Token);
        var ack3 = await publisher.PublishAckAsync(topic, payload3, ct: cts.Token);

        ack1.Accepted.Should().BeTrue();
        ack2.Accepted.Should().BeTrue();
        ack3.Accepted.Should().BeTrue();

        // Now subscribe from offset 0 using the ITopicStore directly
        var messages = _factory.Store.SubscribeAsync(topic, 0, cts.Token);

        var received = new List<StoredMessage>();
        await foreach (var msg in messages.WithCancellation(cts.Token))
        {
            received.Add(msg);
            if (received.Count == 3) break;
        }

        received.Count.Should().Be(3);
        // Verify payloads are byte-exact and matches
        received.Any(m => m.Payload.Span.SequenceEqual(payload1)).Should().BeTrue();
        received.Any(m => m.Payload.Span.SequenceEqual(payload2)).Should().BeTrue();
        received.Any(m => m.Payload.Span.SequenceEqual(payload3)).Should().BeTrue();
    }
}
