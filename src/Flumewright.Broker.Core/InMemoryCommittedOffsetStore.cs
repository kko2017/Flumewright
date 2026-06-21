using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

public sealed class InMemoryCommittedOffsetStore : ICommittedOffsetStore
{
    private readonly ITopicStore _topicStore;
    
    // Key: (groupId, topic, partition)
    private readonly Dictionary<(string, string, int), long> _offsets = new();
    
    // Lock for range + backwards-commit check + update (no check-then-act)
    private readonly object _lock = new();

    public InMemoryCommittedOffsetStore(ITopicStore topicStore)
    {
        _topicStore = topicStore;
    }

    public ValueTask<(bool Ok, string Reason)> CommitOffsetAsync(string groupId, string topic, int partition, long offset, CancellationToken ct = default)
    {
        if (offset < 0)
        {
            return new ValueTask<(bool, string)>((false, "Offset cannot be negative"));
        }

        var key = (groupId, topic, partition);

        lock (_lock)
        {
            long highWatermark = _topicStore.GetPartitionHighWatermark(topic, partition);
            
            // DEC-023: semantics (B) Kafka-style. Committed offset is the NEXT offset to read.
            // Valid range is [0, highWatermark]. Committing exactly highWatermark is valid 
            // ("all current records processed").
            if (offset > highWatermark)
            {
                return new ValueTask<(bool, string)>((false, "Offset out of range"));
            }

            if (_offsets.TryGetValue(key, out var current) && offset < current)
            {
                return new ValueTask<(bool, string)>((false, "Backwards commit rejected"));
            }

            _offsets[key] = offset;
        }

        return new ValueTask<(bool, string)>((true, ""));
    }

    public ValueTask<long?> GetCommittedOffsetAsync(string groupId, string topic, int partition, CancellationToken ct = default)
    {
        var key = (groupId, topic, partition);
        
        lock (_lock)
        {
            if (_offsets.TryGetValue(key, out var offset))
            {
                return new ValueTask<long?>(offset);
            }
        }
        
        return new ValueTask<long?>((long?)null);
    }
}
