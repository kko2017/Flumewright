using Flumewright.Protocol;
using Grpc.Net.Client;
using Google.Protobuf;
using System;

namespace Flumewright.Client;

public sealed class FlumewrightPublisher : IDisposable
{
    private readonly GrpcChannel _channel;
    private readonly MessageBus.MessageBusClient _client;

    public FlumewrightPublisher(string address)
    {
        _channel = GrpcChannel.ForAddress(address);
        _client = new MessageBus.MessageBusClient(_channel);
    }

    public async Task<long> PublishAsync(
        string topic,
        byte[] payload,
        byte[]? partitionKey = null,
        IReadOnlyDictionary<string, string>? headers = null,
        string? clientMsgId = null,
        CancellationToken ct = default)
    {
        var ack = await PublishAckAsync(topic, payload, partitionKey, headers, clientMsgId, ct);
        return ack.Offset;
    }

    public async Task<PublishAck> PublishAckAsync(
        string topic,
        byte[] payload,
        byte[]? partitionKey = null,
        IReadOnlyDictionary<string, string>? headers = null,
        string? clientMsgId = null,
        CancellationToken ct = default)
    {
        var envelope = new PublishEnvelope
        {
            Topic = topic,
            Payload = ByteString.CopyFrom(payload),
            ClientMsgId = clientMsgId ?? Guid.NewGuid().ToString()
        };
        if (partitionKey is not null)
        {
            envelope.PartitionKey = ByteString.CopyFrom(partitionKey);
        }
        if (headers is not null)
        {
            foreach (var kv in headers)
            {
                envelope.Headers[kv.Key] = kv.Value;
            }
        }

        return await _client.PublishAsync(envelope, cancellationToken: ct);
    }

    public void Dispose() => _channel.Dispose();
}
