using Flumewright.Protocol;
using Flumewright.Broker.Core;
using Grpc.Core;
using Google.Protobuf;

namespace Flumewright.Broker.Services;

public class MessageBusService : MessageBus.MessageBusBase
{
    private readonly ITopicStore _topicStore;

    public MessageBusService(ITopicStore topicStore)
    {
        _topicStore = topicStore;
    }

    public override async Task<PublishAck> Publish(PublishEnvelope request, ServerCallContext context)
    {
        var headers = new Dictionary<string, string>();
        foreach (var header in request.Headers)
        {
            headers[header.Key] = header.Value;
        }

        var (partition, offset) = await _topicStore.PublishAsync(
            request.Topic,
            request.PartitionKey.Memory,
            headers,
            request.Payload.Memory,
            context.CancellationToken);

        return new PublishAck
        {
            ClientMsgId = request.ClientMsgId,
            Topic = request.Topic,
            Offset = offset,
            Partition = partition,
            Accepted = true
        };
    }

    public override async Task Subscribe(SubscribeRequest request, IServerStreamWriter<DeliverEnvelope> responseStream, ServerCallContext context)
    {
        var messages = _topicStore.SubscribeAsync(request.Topic, context.CancellationToken);

        await foreach (var message in messages)
        {
            var envelope = new DeliverEnvelope
            {
                Topic = request.Topic,
                Offset = message.Offset,
                Partition = message.Partition,
                Payload = ByteString.CopyFrom(message.Payload.Span)
            };

            foreach (var header in message.Headers)
            {
                envelope.Headers[header.Key] = header.Value;
            }

            await responseStream.WriteAsync(envelope);
        }
    }
}
