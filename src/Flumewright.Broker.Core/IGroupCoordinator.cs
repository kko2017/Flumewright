using System.Collections.Generic;

namespace Flumewright.Broker.Core;

public enum GroupState
{
    Stable,
    PreparingRebalance,
    CompletingRebalance
}

public record TopicPartition(string Topic, int Partition);

public record GroupMemberSnapshot(string MemberId, IReadOnlyList<string> Topics, IReadOnlyList<TopicPartition> AssignedPartitions);

public record GroupStateSnapshot(string GroupId, int Generation, GroupState State, string? LeaderId, IReadOnlyList<GroupMemberSnapshot> Members);

public record GroupJoinResult(int Generation, GroupState State, bool IsLeader, IReadOnlyList<GroupMemberSnapshot> Members);

public record GroupSyncResult(int Generation, IReadOnlyList<TopicPartition> AssignedPartitions);

public interface IGroupCoordinator
{
    // Membership operations
    Task<GroupJoinResult> JoinGroupAsync(string groupId, string memberId, IReadOnlyList<string> topics, TimeSpan rebalanceTimeout, CancellationToken ct);
    Task<GroupSyncResult> SyncGroupAsync(string groupId, string memberId, int generation, IReadOnlyDictionary<string, IReadOnlyList<TopicPartition>> assignments, CancellationToken ct);
    
    bool RemoveMember(string groupId, string memberId);
    
    
    // Heartbeat
    bool RecordHeartbeat(string groupId, string memberId, int expectedGeneration, out bool rebalanceInProgress);
    void SweepDeadMembers(TimeSpan sessionTimeout);

    GroupStateSnapshot? GetGroupState(string groupId);
}
