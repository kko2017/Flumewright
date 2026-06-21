using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

public interface ICommittedOffsetStore
{
    ValueTask<(bool Ok, string Reason)> CommitOffsetAsync(string groupId, string topic, int partition, long offset);
    ValueTask<long?> GetCommittedOffsetAsync(string groupId, string topic, int partition);
}
