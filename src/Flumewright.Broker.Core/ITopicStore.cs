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

    /// <summary>
    /// Reads a specific partition starting from <paramref name="startOffset"/>.
    /// If <paramref name="startOffset"/> is negative, it resolves atomically to the current high watermark (LATEST).
    /// </summary>
    IAsyncEnumerable<StoredMessage> ReadPartitionAsync(
        string topic,
        int partition,
        long startOffset,
        CancellationToken ct = default);

    IAsyncEnumerable<StoredMessage> ReadPartitionsAsync(
        string topic,
        IReadOnlyDictionary<int, long> partitionOffsets,
        CancellationToken ct = default);

    long? GetPartitionHighWatermark(string topic, int partition);
}
