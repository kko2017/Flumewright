using Flumewright.Protocol;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Runtime.CompilerServices;

namespace Flumewright.Client;

public sealed record ReceivedMessage(
    string Topic, long Offset,IReadOnlyDictionary<string,string> Headers, byte[] Paylaod);

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
            yield return new ReceivedMessage(d.Topic, d.Offset, new Dictionary<string,string>(d.Headers), d.Payload.ToByteArray());
        }
    }

    public void Dispose() => _channel.Dispose();
}
