using Flumewright.Protocol;
using Flumewright.Broker.Core;
using Grpc.Core;
using Google.Protobuf;

namespace Flumewright.Broker.Services;

public class MessageBusService : MessageBus.MessageBusBase
{
    private readonly ITopicStore _topicStore;
    private readonly ICommittedOffsetStore _offsetStore;

    public MessageBusService(ITopicStore topicStore, ICommittedOffsetStore offsetStore)
    {
        _topicStore = topicStore;
        _offsetStore = offsetStore;
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
        if (request.Partitions.Count == 0)
        {
            // Fallback for M1/M2 behavior if needed, or we just return.
            // The instruction says "Subscribe must respect the consumer's declared `partitions` (static assignment) — stream only those partitions, not all of them. (This is the M3a change from M2's "read all partitions".)"
            // Let's assume the request always provides partitions now. If empty, stream nothing.
            return;
        }

        var channel = System.Threading.Channels.Channel.CreateUnbounded<StoredMessage>(new System.Threading.Channels.UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true
        });

        var partitionTasks = new List<Task>();

        foreach (var p in request.Partitions)
        {
            int partition = p;
            var task = Task.Run(async () =>
            {
                try
                {
                    long startOffset;
                    var committed = await _offsetStore.GetCommittedOffsetAsync(request.GroupId, request.Topic, partition, context.CancellationToken);
                    
                    if (committed.HasValue)
                    {
                        startOffset = committed.Value; // DEC-023: committed is the next offset to read
                    }
                    else
                    {
                        startOffset = request.Reset == OffsetReset.Earliest ? 0 : -1; // -1 matches M2's "from now"
                    }

                    // For LATEST (-1), ReadPartitionAsync natively might not handle -1 like SubscribeAsync does.
                    // Wait, let's look at ReadPartitionAsync. Oh, SubscribeAsync handles -1 before calling ReadFromOffsetAsync!
                    if (startOffset < 0)
                    {
                        var hw = _topicStore.GetPartitionHighWatermark(request.Topic, partition);
                        startOffset = hw ?? 0;
                    }

                    await foreach (var msg in _topicStore.ReadPartitionAsync(request.Topic, partition, startOffset, context.CancellationToken))
                    {
                        await channel.Writer.WriteAsync(msg, context.CancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Cancellation is normal shutdown behavior for this partition reader task.
                }
            }, CancellationToken.None);

            partitionTasks.Add(task);
        }

        _ = Task.WhenAll(partitionTasks).ContinueWith(t => channel.Writer.TryComplete(t.Exception), TaskScheduler.Default);

        while (await channel.Reader.WaitToReadAsync(context.CancellationToken))
        {
            while (channel.Reader.TryRead(out var message))
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

                await responseStream.WriteAsync(envelope, context.CancellationToken);
            }
        }
    }

    public override async Task<CommitAck> CommitOffset(CommitRequest request, ServerCallContext context)
    {
        var (ok, reason) = await _offsetStore.CommitOffsetAsync(
            request.GroupId, request.Topic, request.Partition, request.Offset, context.CancellationToken);

        return new CommitAck
        {
            Ok = ok,
            Reason = reason ?? string.Empty
        };
    }
}
