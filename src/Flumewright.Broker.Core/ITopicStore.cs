namespace Flumewright.Broker.Core;

public interface ITopicStore
{
    ValueTask<long> PublishAsync(
        string topic,
        IReadOnlyDictionary<string, string> headers,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default);

    IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        CancellationToken ct = default);
}
