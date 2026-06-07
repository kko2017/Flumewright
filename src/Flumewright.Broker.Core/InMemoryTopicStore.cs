using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Flumewright.Broker.Core;

public sealed class InMemoryTopicStore : ITopicStore
{
    private class TopicPartition
    {
        public Channel<StoredMessage> Channel { get; } = System.Threading.Channels.Channel.CreateUnbounded<StoredMessage>();
        private long _offsetCounter = -1;

        public long GetNextOffset() => Interlocked.Increment(ref _offsetCounter);
        public long CurrentOffset => Interlocked.Read(ref _offsetCounter);
    }

    private readonly ConcurrentDictionary<string, TopicPartition> _topics = new();

    public async ValueTask<long> PublishAsync(string topic, IReadOnlyDictionary<string, string> headers, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var partition = _topics.GetOrAdd(topic, _ => new TopicPartition());
        long offset = partition.GetNextOffset();
        var message = new StoredMessage(offset, headers, payload);
        await partition.Channel.Writer.WriteAsync(message, ct);
        return offset;
    }

    public async IAsyncEnumerable<StoredMessage> SubscribeAsync(string topic, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var partition = _topics.GetOrAdd(topic, _ => new TopicPartition());
        long startOffset = partition.CurrentOffset;

        await foreach (var msg in partition.Channel.Reader.ReadAllAsync(ct))
        {
            if (msg.Offset > startOffset)
            {
                yield return msg;
            }
        }
    }
}
