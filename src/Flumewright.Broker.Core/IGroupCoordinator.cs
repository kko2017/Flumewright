using System.Collections.Generic;

namespace Flumewright.Broker.Core;

public enum GroupState
{
    Stable,
    Rebalancing
}

public record TopicPartition(string Topic, int Partition);

public record GroupMemberSnapshot(string MemberId, IReadOnlyList<string> Topics, IReadOnlyList<TopicPartition> AssignedPartitions);

public record GroupStateSnapshot(string GroupId, int Generation, GroupState State, string? LeaderId, IReadOnlyList<GroupMemberSnapshot> Members);

public record GroupJoinResult(int Generation, GroupState State, bool IsLeader);

public interface IGroupCoordinator
{
    // Membership operations
    GroupJoinResult AddOrUpdateMember(string groupId, string memberId, IReadOnlyList<string> topics);
    bool RemoveMember(string groupId, string memberId);
    
    // Rebalance lifecycle
    bool BeginRebalance(string groupId);
    bool CompleteRebalance(string groupId, int expectedGeneration, IReadOnlyDictionary<string, IReadOnlyList<TopicPartition>> assignments);
    
    // Heartbeat
    bool RecordHeartbeat(string groupId, string memberId, int expectedGeneration, out bool rebalanceInProgress);
    void SweepDeadMembers(TimeSpan sessionTimeout);

    GroupStateSnapshot? GetGroupState(string groupId);
}
