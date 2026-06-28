using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

internal sealed class InMemoryCommittedOffsetStore : ICommittedOffsetStore
{
    private readonly ITopicStore _topicStore;
    
    // Key: (groupId, topic, partition)
    private readonly Dictionary<(string, string, int), long> _offsets = new();
    
    // Lock for range + backwards-commit check + update (no check-then-act)
    private readonly object _lock = new();

    private readonly IGroupCoordinator? _coordinator;

    public InMemoryCommittedOffsetStore(ITopicStore topicStore, IGroupCoordinator? coordinator = null)
    {
        _topicStore = topicStore;
        _coordinator = coordinator;
    }

    public ValueTask<(bool Ok, string Reason, Flumewright.Protocol.GroupErrorCode Code)> CommitOffsetAsync(string groupId, string topic, int partition, long offset, int generation = 0, CancellationToken ct = default)
    {
        if (offset < 0)
        {
            return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((false, GroupMessages.OffsetCannotBeNegative, Flumewright.Protocol.GroupErrorCode.GroupOk));
        }

        var key = (groupId, topic, partition);

        lock (_lock)
        {
            if (generation > 0 && _coordinator != null)
            {
                var currentGen = _coordinator.GetGroupGeneration(groupId);
                if (currentGen != null && currentGen.Value != generation)
                {
                    return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((false, GroupMessages.FencedStaleGeneration, Flumewright.Protocol.GroupErrorCode.GroupFenced));
                }
            }
            // Reading the watermark MUST be inside the lock. If read before the lock, a message 
            // could be published (increasing the watermark) before we acquire the lock, causing 
            // a valid commit of the new watermark to be falsely rejected as out-of-range against 
            // the stale read (a check-then-act race). This coarse global lock is intentional for Phase 1.
            long? highWatermarkOpt = _topicStore.GetPartitionHighWatermark(topic, partition);
            if (!highWatermarkOpt.HasValue)
            {
                // (a) A commit acks records actually read; a topic/partition that was never published was never
                // read, so reject. The (b) variant — allowing commit(0) on a pre-created empty topic — is
                // deferred; switching to it would require a new DEC and an API to pre-create empty topics.
                // See DEC-023.
                return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((false, GroupMessages.UnknownTopicOrInvalidPartition, Flumewright.Protocol.GroupErrorCode.GroupOk));
            }
            long highWatermark = highWatermarkOpt.Value;
            
            // DEC-023: semantics (B) Kafka-style. Committed offset is the NEXT offset to read.
            // Valid range is [0, highWatermark]. Committing exactly highWatermark is valid 
            // ("all current records processed").
            if (offset > highWatermark)
            {
                return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((false, GroupMessages.OffsetOutOfRange, Flumewright.Protocol.GroupErrorCode.GroupOk));
            }

            if (_offsets.TryGetValue(key, out var current) && offset < current)
            {
                return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((false, GroupMessages.BackwardsCommitRejected, Flumewright.Protocol.GroupErrorCode.GroupOk));
            }

            _offsets[key] = offset;
        }

        return new ValueTask<(bool, string, Flumewright.Protocol.GroupErrorCode)>((true, "", Flumewright.Protocol.GroupErrorCode.GroupOk));
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
