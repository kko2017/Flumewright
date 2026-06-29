using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

public interface ICommittedOffsetStore
{
    ValueTask<(bool Ok, string Reason, Flumewright.Protocol.GroupErrorCode Code)> CommitOffsetAsync(string groupId, string topic, int partition, long offset, int generation = 0, CancellationToken ct = default);
    ValueTask<long?> GetCommittedOffsetAsync(string groupId, string topic, int partition, CancellationToken ct = default);
}
