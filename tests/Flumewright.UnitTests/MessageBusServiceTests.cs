using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Flumewright.Broker.Core;
using Flumewright.Broker.Services;
using Flumewright.Protocol;
using Grpc.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class MessageBusServiceTests
{
    private class FakeServerStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }
        public List<T> Items { get; } = new();

        public Task WriteAsync(T message)
        {
            Items.Add(message);
            return Task.CompletedTask;
        }
    }

    private class FakeServerCallContext : ServerCallContext
    {
        protected override string MethodCore => "Fake";
        protected override string HostCore => "Fake";
        protected override string PeerCore => "Fake";
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => new Metadata();
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore => new Metadata();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => null!;
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_EmptyPartitionsWithGroup_ThrowsInvalidArgument()
    {
        var topicStore = new InMemoryTopicStore(1);
        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        var service = new MessageBusService(topicStore, offsetStore, new Flumewright.Broker.Core.GroupCoordinator());

        var request = new SubscribeRequest { Topic = "t1", GroupId = "g1" };
        var writer = new FakeServerStreamWriter<DeliverEnvelope>();
        var context = new FakeServerCallContext();

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Subscribe(request, writer, context));
        ex.StatusCode.Should().Be(StatusCode.InvalidArgument);
        ex.Status.Detail.Should().Contain("Group subscribe requires explicit partitions");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public async Task Subscribe_InvalidPartition_ThrowsInvalidArgument()
    {
        var topicStore = new InMemoryTopicStore(1); // Only partition 0 exists
        var offsetStore = new InMemoryCommittedOffsetStore(topicStore);
        var service = new MessageBusService(topicStore, offsetStore, new Flumewright.Broker.Core.GroupCoordinator());

        var request = new SubscribeRequest { Topic = "t1", GroupId = "g1" };
        request.Partitions.Add(99); // Invalid partition
        var writer = new FakeServerStreamWriter<DeliverEnvelope>();
        var context = new FakeServerCallContext();

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Subscribe(request, writer, context));
        ex.StatusCode.Should().Be(StatusCode.InvalidArgument);
        ex.Status.Detail.Should().Contain("Unknown topic or invalid partition");
    }
}
