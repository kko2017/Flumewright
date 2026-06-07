using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Flumewright.Broker.Core;

public sealed class InMemoryTopicStore : ITopicStore
{
    private class Topic
    {
        public ConcurrentDictionary<Guid, Channel<StoredMessage>> Subscribers { get; } = new();
        private long _offsetCounter = -1;

        public long GetNextOffset() => Interlocked.Increment(ref _offsetCounter);
    }

    private readonly ConcurrentDictionary<string, Topic> _topics = new();

    public ValueTask<long> PublishAsync(string topic, IReadOnlyDictionary<string, string> headers, ReadOnlyMemory<byte> payload, CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic());
        long offset = topicState.GetNextOffset();
        var message = new StoredMessage(offset, headers, payload);
        
        foreach (var sub in topicState.Subscribers.Values)
        {
            sub.Writer.TryWrite(message);
        }
        
        return new ValueTask<long>(offset);
    }

    public async IAsyncEnumerable<StoredMessage> SubscribeAsync(string topic, [EnumeratorCancellation] CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic());
        var subId = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<StoredMessage>();
        
        topicState.Subscribers.TryAdd(subId, channel);

        try
        {
            await foreach (var msg in channel.Reader.ReadAllAsync(ct))
            {
                yield return msg;
            }
        }
        finally
        {
            topicState.Subscribers.TryRemove(subId, out _);
        }
    }
}
