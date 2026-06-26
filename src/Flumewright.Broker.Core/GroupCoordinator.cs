using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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

    private void StartRebalance(ConsumerGroup group)
    {
        group.State = GroupState.Rebalancing;
        group.Generation++;
        group.LeaderId = null;
        
        group.RebalanceTcs?.TrySetCanceled();
        group.SyncTcs?.TrySetCanceled();
        
        group.RebalanceTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        group.SyncTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        foreach (var m in group.Members.Values)
        {
            m.HasJoined = false;
            m.AssignedPartitions = Array.Empty<TopicPartition>();
        }
    }

    public async Task<GroupJoinResult> JoinGroupAsync(string groupId, string memberId, IReadOnlyList<string> topics, TimeSpan rebalanceTimeout, CancellationToken ct)
    {
        Task waitTask;
        int generation;

        lock (_lock)
        {
            var group = GetOrAddGroup(groupId);
            
            bool isNewOrChanged = false;
            if (!group.Members.TryGetValue(memberId, out var member))
            {
                isNewOrChanged = true;
            }
            else if (!TopicsEqual(member.Topics, topics))
            {
                isNewOrChanged = true;
            }

            if (group.State == GroupState.Stable && isNewOrChanged)
            {
                StartRebalance(group);
            }
            // Even if not new or changed, if they are calling JoinGroup, they are joining the rebalance.
            // If the state is Stable and not new/changed, wait, why would they call JoinGroup unless rebalancing?
            // If they just call JoinGroup randomly, it should trigger a rebalance.
            else if (group.State == GroupState.Stable)
            {
                StartRebalance(group);
            }

            if (member == null)
            {
                member = new GroupMember(memberId, topics);
                group.Members[memberId] = member;
            }
            else
            {
                member.Topics = topics;
                member.LastHeartbeat = DateTimeOffset.UtcNow;
            }
            
            member.HasJoined = true;
            
            if (group.LeaderId == null)
            {
                group.LeaderId = memberId;
            }
            
            if (group.RebalanceTcs == null)
            {
                 group.RebalanceTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            generation = group.Generation;
            waitTask = group.RebalanceTcs.Task;

            if (group.Members.Values.All(m => m.HasJoined))
            {
                group.RebalanceTcs.TrySetResult();
            }
        }

        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(rebalanceTimeout);
            await waitTask.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            lock (_lock)
            {
                var group = GetOrAddGroup(groupId);
                if (group.Generation == generation && group.State == GroupState.Rebalancing && group.RebalanceTcs != null && !group.RebalanceTcs.Task.IsCompleted)
                {
                    var dead = group.Members.Values.Where(m => !m.HasJoined).Select(m => m.MemberId).ToList();
                    foreach (var d in dead) group.Members.Remove(d);
                    
                    if (group.LeaderId != null && dead.Contains(group.LeaderId))
                    {
                        using var enumerator = group.Members.Keys.GetEnumerator();
                        group.LeaderId = enumerator.MoveNext() ? enumerator.Current : null;
                    }

                    group.RebalanceTcs.TrySetResult();
                }
            }
            
            try
            {
                await waitTask;
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException("Rebalance in progress");
            }
        }

        lock (_lock)
        {
            var group = GetOrAddGroup(groupId);
            bool isLeader = (group.LeaderId == memberId);
            var membersList = new List<GroupMemberSnapshot>();
            if (isLeader)
            {
                foreach(var m in group.Members.Values)
                {
                    membersList.Add(new GroupMemberSnapshot(m.MemberId, m.Topics, Array.Empty<TopicPartition>()));
                }
                
                if (group.SyncTcs == null || group.SyncTcs.Task.IsCompleted || group.SyncTcs.Task.IsCanceled)
                {
                    group.SyncTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }
            }
            return new GroupJoinResult(group.Generation, group.State, isLeader, membersList);
        }
    }

    public async Task<GroupSyncResult> SyncGroupAsync(string groupId, string memberId, int generation, IReadOnlyDictionary<string, IReadOnlyList<TopicPartition>> assignments, CancellationToken ct)
    {
        Task syncTask;
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) throw new InvalidOperationException("Group not found");
            if (group.Generation != generation) throw new InvalidOperationException("Fenced");
            
            if (group.LeaderId == memberId && group.State == GroupState.Rebalancing)
            {
                foreach (var kvp in assignments)
                {
                    if (group.Members.TryGetValue(kvp.Key, out var member))
                    {
                        member.AssignedPartitions = kvp.Value;
                    }
                }
                group.State = GroupState.Stable;
                group.SyncTcs?.TrySetResult();
            }
            
            syncTask = group.SyncTcs?.Task ?? Task.CompletedTask;
        }

        try
        {
            await syncTask.WaitAsync(ct);
        }
        catch (OperationCanceledException)
        {
            throw new InvalidOperationException("Rebalance in progress");
        }

        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) throw new InvalidOperationException("Group not found");
            if (!group.Members.TryGetValue(memberId, out var member)) throw new InvalidOperationException("Member not found");
            if (group.Generation != generation) throw new InvalidOperationException("Fenced");

            return new GroupSyncResult(group.Generation, member.AssignedPartitions);
        }
    }

    public bool RemoveMember(string groupId, string memberId)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return false;

            if (group.Members.Remove(memberId))
            {
                StartRebalance(group);
                return true;
            }
            return false;
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
                    StartRebalance(group);
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

        public TaskCompletionSource? RebalanceTcs { get; set; }
        public TaskCompletionSource? SyncTcs { get; set; }

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
        public bool HasJoined { get; set; }

        public GroupMember(string memberId, IReadOnlyList<string> topics)
        {
            MemberId = memberId;
            Topics = topics;
            LastHeartbeat = DateTimeOffset.UtcNow;
        }
    }
}
