using Flumewright.Protocol;
using Flumewright.Broker.Core;
using Grpc.Core;
using Google.Protobuf;

namespace Flumewright.Broker.Services;

public class MessageBusService : MessageBus.MessageBusBase
{
    private readonly ITopicStore _topicStore;
    private readonly ICommittedOffsetStore _offsetStore;
    private readonly IGroupCoordinator _groupCoordinator;

    public MessageBusService(ITopicStore topicStore, ICommittedOffsetStore offsetStore, IGroupCoordinator groupCoordinator)
    {
        _topicStore = topicStore;
        _offsetStore = offsetStore;
        _groupCoordinator = groupCoordinator;
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
        if (string.IsNullOrEmpty(request.GroupId))
        {
            var messages = _topicStore.SubscribeAsync(request.Topic, context.CancellationToken);
            await StreamToClientAsync(messages, responseStream, request.Topic, context.CancellationToken);
            return;
        }

        if (request.Partitions.Count == 0)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Group subscribe requires explicit partitions."));
        }

        foreach (var p in request.Partitions)   // NOSONAR imperative validation-then-throw is clearer than a LINQ filter here
        {
            if (_topicStore.GetPartitionHighWatermark(request.Topic, p) == null)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, $"Unknown topic or invalid partition: {request.Topic}:{p}"));
            }
        }

        var partitionOffsets = await BuildPartitionOffsetsAsync(request, context.CancellationToken);
        var partitionMessages = _topicStore.ReadPartitionsAsync(request.Topic, partitionOffsets, context.CancellationToken);
        await StreamToClientAsync(partitionMessages, responseStream, request.Topic, context.CancellationToken);
    }

    private async Task<Dictionary<int, long>> BuildPartitionOffsetsAsync(SubscribeRequest request, CancellationToken ct)
    {
        var partitionOffsets = new Dictionary<int, long>();
        foreach (var p in request.Partitions)
        {
            var committed = await _offsetStore.GetCommittedOffsetAsync(request.GroupId, request.Topic, p, ct);
            if (committed.HasValue)
            {
                partitionOffsets[p] = committed.Value; // DEC-023: committed is the next offset to read
            }
            else
            {
                partitionOffsets[p] = request.Reset == OffsetReset.Earliest ? 0 : -1; // -1 atomically resolves to LATEST in the store
            }
        }
        return partitionOffsets;
    }

    private static async Task StreamToClientAsync(IAsyncEnumerable<StoredMessage> source, IServerStreamWriter<DeliverEnvelope> responseStream, string topic, CancellationToken ct)
    {
        await foreach (var message in source.WithCancellation(ct))
        {
            var envelope = new DeliverEnvelope
            {
                Topic = topic,
                Offset = message.Offset,
                Partition = message.Partition,
                Payload = ByteString.CopyFrom(message.Payload.Span)
            };

            foreach (var header in message.Headers)
            {
                envelope.Headers[header.Key] = header.Value;
            }

            await responseStream.WriteAsync(envelope, ct);
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

    public override Task<HeartbeatResponse> Heartbeat(HeartbeatRequest request, ServerCallContext context)
    {
        bool ok = _groupCoordinator.RecordHeartbeat(
            request.GroupId, 
            request.MemberId, 
            request.Generation, 
            out bool rebalanceInProgress);

        return Task.FromResult(new HeartbeatResponse
        {
            Ok = ok,
            Reason = ok ? string.Empty : "Invalid generation or member unknown",
            RebalanceInProgress = rebalanceInProgress
        });
    }
}
