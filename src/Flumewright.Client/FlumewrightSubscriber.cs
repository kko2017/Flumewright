using Flumewright.Protocol;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Runtime.CompilerServices;

namespace Flumewright.Client;

public sealed record ReceivedMessage(
    string Topic, long Offset, IReadOnlyDictionary<string, string> Headers, byte[] Payload, int Partition);

public enum FlumewrightOffsetReset
{
    Earliest = 0,
    Latest = 1
}

public sealed class FlumewrightSubscriber: IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly MessageBus.MessageBusClient _client;

    public FlumewrightSubscriber(string address)
    {
        _channel = GrpcChannel.ForAddress(address);
        _client = new MessageBus.MessageBusClient(_channel);
    }

    public async IAsyncEnumerable<ReceivedMessage> SubscribeAsync(
        string topic,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        using var call = _client.Subscribe(new SubscribeRequest { Topic = topic }, cancellationToken: ct);
        await foreach (var d in call.ResponseStream.ReadAllAsync(ct))
        {
            yield return new ReceivedMessage(d.Topic, d.Offset, new Dictionary<string, string>(d.Headers), d.Payload.ToByteArray(), d.Partition);
        }
    }

    public async IAsyncEnumerable<ReceivedMessage> SubscribeGroupAsync(
        string topic,
        string groupId,
        IReadOnlyList<int> partitions,
        FlumewrightOffsetReset reset = FlumewrightOffsetReset.Earliest,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        var req = new SubscribeRequest 
        { 
            Topic = topic,
            GroupId = groupId,
            Reset = reset == FlumewrightOffsetReset.Earliest ? OffsetReset.Earliest : OffsetReset.Latest
        };
        req.Partitions.AddRange(partitions);

        using var call = _client.Subscribe(req, cancellationToken: ct);
        await foreach (var d in call.ResponseStream.ReadAllAsync(ct))
        {
            yield return new ReceivedMessage(d.Topic, d.Offset, new Dictionary<string, string>(d.Headers), d.Payload.ToByteArray(), d.Partition);
        }
    }

    public async Task CommitOffsetAsync(string groupId, string topic, int partition, long offset, CancellationToken ct = default)
    {
        var req = new CommitRequest
        {
            GroupId = groupId,
            Topic = topic,
            Partition = partition,
            Offset = offset
        };

        var ack = await _client.CommitOffsetAsync(req, cancellationToken: ct);
        if (!ack.Ok)
        {
            throw new Exception($"Commit failed: {ack.Reason}");
        }
    }

    public void Dispose() => _channel.Dispose();
}
