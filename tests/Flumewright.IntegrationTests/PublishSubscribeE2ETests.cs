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
        // (4b 스위치가 필요했다면 여기 첫 채널 생성 전에 한 번 — 08 Step4 발견사항 참조)
         //AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        var address = _factory.Address;    // http://127.0.0.1:{port}
        const string topic = "it.demo";
        var payload = System.Text.Encoding.UTF8.GetBytes("hello-e2e");

        using var subscriber = new FlumewrightSubscriber(address);
        using var publisher = new FlumewrightPublisher(address);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // 함정② 대응: 구독을 백그라운드로 먼저 시작해 스트림을 확립한다.
        var received = new TaskCompletionSource<ReceivedMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

        var pump = Task.Run(async () =>
        {
            await foreach (var msg in subscriber.SubscribeAsync(topic, cts.Token))
            {
                received.TrySetResult(msg);
                break; // 1건이면 충분
            }
        }, cts.Token);

        // 구독 스트림이 broker에 등록될 시간을 안정적으로 확보:
        // (간단·견고하게: 짧게 재시도하며 publish — LATEST라 첫 시도가 빠르면 놓칠 수 있으므로
        //  도착할 때까지 몇 번 재발행. Delay 단독 대신 "재발행 + 타임아웃 대기" 조합으로 flaky 차단.)
        long lastOffset = -1;
        while(!received.Task.IsCompleted && !cts.IsCancellationRequested)
        {
            lastOffset = await publisher.PublishAsync(topic, payload, ct: cts.Token);
            var done = await Task.WhenAny(received.Task, Task.Delay(200, cts.Token));
            if (done == received.Task) break;
        }

        var got = await received.Task;

        got.Topic.Should().Be(topic);
        got.Payload.Should().Equal(payload);
        lastOffset.Should().BeGreaterThanOrEqualTo(0);
    }
}
