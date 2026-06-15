using FluentAssertions;
using Flumewright.Client;
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
}
