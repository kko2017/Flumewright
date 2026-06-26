using System;
using System.Collections.Generic;

namespace Flumewright.Broker.Core;

public class GroupCoordinator : IGroupCoordinator
{
    private readonly object _lock = new();
    private readonly Dictionary<string, ConsumerGroup> _groups = new();

    private ConsumerGroup GetOrAddGroup(string groupId)
    {
        if (!_groups.TryGetValue(groupId, out var group))
        {
            group = new ConsumerGroup(groupId);
            _groups[groupId] = group;
        }
        return group;
    }

    public GroupJoinResult AddOrUpdateMember(string groupId, string memberId, IReadOnlyList<string> topics)
    {
        lock (_lock)
        {
            var group = GetOrAddGroup(groupId);
            bool isNewOrChanged = false;

            if (!group.Members.TryGetValue(memberId, out var member))
            {
                member = new GroupMember(memberId, topics);
                group.Members[memberId] = member;
                isNewOrChanged = true;
            }
            else
            {
                member.LastHeartbeat = DateTimeOffset.UtcNow;
                if (!TopicsEqual(member.Topics, topics))
                {
                    member.Topics = topics;
                    isNewOrChanged = true;
                }
            }

            if (isNewOrChanged)
            {
                group.Generation++;
                group.State = GroupState.Rebalancing;
                
                foreach (var m in group.Members.Values)
                {
                    m.AssignedPartitions = Array.Empty<TopicPartition>();
                }
            }

            if (group.LeaderId == null || !group.Members.ContainsKey(group.LeaderId))
            {
                using var enumerator = group.Members.Keys.GetEnumerator();
                group.LeaderId = enumerator.MoveNext() ? enumerator.Current : null;
            }

            return new GroupJoinResult(group.Generation, group.State, group.LeaderId == memberId);
        }
    }

    public bool RemoveMember(string groupId, string memberId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;

            if (group.Members.Remove(memberId))
            {
                group.Generation++;
                group.State = GroupState.Rebalancing;
                
                foreach (var m in group.Members.Values)
                {
                    m.AssignedPartitions = Array.Empty<TopicPartition>();
                }
                
                if (group.LeaderId == memberId)
                {
                    using var enumerator = group.Members.Keys.GetEnumerator();
                    group.LeaderId = enumerator.MoveNext() ? enumerator.Current : null;
                }
                return true;
            }
            return false;
        }
    }

    public bool BeginRebalance(string groupId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;
            
            if (group.State != GroupState.Rebalancing)
            {
                group.State = GroupState.Rebalancing;
                group.Generation++;
                
                foreach (var m in group.Members.Values)
                {
                    m.AssignedPartitions = Array.Empty<TopicPartition>();
                }
                return true;
            }
            return false;
        }
    }

    public bool CompleteRebalance(string groupId, int expectedGeneration, IReadOnlyDictionary<string, IReadOnlyList<TopicPartition>> assignments)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;
            
            if (group.Generation != expectedGeneration) return false;
            if (group.State != GroupState.Rebalancing) return false;

            foreach (var kvp in assignments)
            {
                if (group.Members.TryGetValue(kvp.Key, out var member))
                {
                    member.AssignedPartitions = kvp.Value;
                }
            }

            group.State = GroupState.Stable;
            return true;
        }
    }

    public bool RecordHeartbeat(string groupId, string memberId, int expectedGeneration, out bool rebalanceInProgress)
    {
        rebalanceInProgress = false;
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;
            if (!group.Members.TryGetValue(memberId, out var member)) return false;
            if (group.Generation != expectedGeneration) return false;

            member.LastHeartbeat = DateTimeOffset.UtcNow;
            rebalanceInProgress = group.State == GroupState.Rebalancing;
            return true;
        }
    }

    public void SweepDeadMembers(TimeSpan sessionTimeout)
    {
        lock (_lock)
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var group in _groups.Values)
            {
                bool anyDead = false;
                var deadMembers = new List<string>();
                
                foreach (var member in group.Members.Values)
                {
                    if (now - member.LastHeartbeat > sessionTimeout)
                    {
                        deadMembers.Add(member.MemberId);
                        anyDead = true;
                    }
                }

                if (anyDead)
                {
                    foreach (var m in deadMembers)
                    {
                        group.Members.Remove(m);
                    }

                    group.Generation++;
                    group.State = GroupState.Rebalancing;
                    foreach (var m in group.Members.Values)
                    {
                        m.AssignedPartitions = Array.Empty<TopicPartition>();
                    }

                    if (group.LeaderId != null && deadMembers.Contains(group.LeaderId))
                    {
                        using var enumerator = group.Members.Keys.GetEnumerator();
                        group.LeaderId = enumerator.MoveNext() ? enumerator.Current : null;
                    }
                }
            }
        }
    }

    public GroupStateSnapshot? GetGroupState(string groupId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return null;

            var members = new List<GroupMemberSnapshot>(group.Members.Count);
            foreach (var m in group.Members.Values)
            {
                members.Add(new GroupMemberSnapshot(m.MemberId, m.Topics, m.AssignedPartitions));
            }
            return new GroupStateSnapshot(groupId, group.Generation, group.State, group.LeaderId, members);
        }
    }

    private bool TopicsEqual(IReadOnlyList<string> a, IReadOnlyList<string> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    private class ConsumerGroup
    {
        public string GroupId { get; }
        public int Generation { get; set; }
        public GroupState State { get; set; }
        public string? LeaderId { get; set; }
        public Dictionary<string, GroupMember> Members { get; } = new();

        public ConsumerGroup(string groupId)
        {
            GroupId = groupId;
            Generation = 0;
            State = GroupState.Stable;
        }
    }

    private class GroupMember
    {
        public string MemberId { get; }
        public IReadOnlyList<string> Topics { get; set; }
        public DateTimeOffset LastHeartbeat { get; set; }
        public IReadOnlyList<TopicPartition> AssignedPartitions { get; set; } = Array.Empty<TopicPartition>();

        public GroupMember(string memberId, IReadOnlyList<string> topics)
        {
            MemberId = memberId;
            Topics = topics;
            LastHeartbeat = DateTimeOffset.UtcNow;
        }
    }
}
