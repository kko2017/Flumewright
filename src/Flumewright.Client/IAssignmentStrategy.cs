using System.Collections.Generic;
using Flumewright.Protocol;

namespace Flumewright.Client;

public interface IAssignmentStrategy
{
    string Name { get; }
    IReadOnlyList<MemberAssignment> Assign(
        IReadOnlyList<MemberMetadata> members,
        IReadOnlyDictionary<string, int> partitionCounts);
}
