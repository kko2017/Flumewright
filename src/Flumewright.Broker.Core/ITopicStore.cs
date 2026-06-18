using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

public interface ITopicStore
{
    ValueTask<(int Partition, long Offset)> PublishAsync(
        string topic,
        ReadOnlyMemory<byte> partitionKey,
        IReadOnlyDictionary<string, string> headers,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default);

    IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        CancellationToken ct = default);

    IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        long startOffset,
        CancellationToken ct = default);

    IAsyncEnumerable<StoredMessage> ReadPartitionAsync(
        string topic,
        int partition,
        long startOffset,
        CancellationToken ct = default);
}
