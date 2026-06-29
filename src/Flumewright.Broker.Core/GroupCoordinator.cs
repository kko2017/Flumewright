using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Broker.Core;

internal record RebalanceOutcome(int Generation, string LeaderId, IReadOnlyList<GroupMemberSnapshot> Members);

internal class GroupCoordinator : IGroupCoordinator
{
    private readonly object _lock = new();
    private readonly ConcurrentDictionary<string, ConsumerGroup> _groups = new();

    private ConsumerGroup GetOrAddGroup(string groupId)
    {
        if (!_groups.TryGetValue(groupId, out var group))
        {
            group = new ConsumerGroup(groupId);
            _groups[groupId] = group;
        }
        return group;
    }

    private static void StartRebalance(ConsumerGroup group)
    {
        group.State = GroupState.PreparingRebalance;
        group.Generation++;
        group.LeaderId = null;
        
        group.RebalanceTcs?.TrySetCanceled();
        group.SyncTcs?.TrySetCanceled();
        
        group.RebalanceTcs = new TaskCompletionSource<RebalanceOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
        group.SyncTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        foreach (var m in group.Members.Values)
        {
            m.HasJoined = false;
            m.AssignedPartitions = Array.Empty<TopicPartition>();
        }
    }

#pragma warning disable S3776 // intentional: concurrency-critical critical section; splitting would obscure lock atomicity / the cancellation-lifetime chain that Coyote (Layer 5) verifies. Correctness over metric.
    public async Task<GroupJoinResult> JoinGroupAsync(string groupId, string memberId, IReadOnlyList<string> topics, TimeSpan rebalanceTimeout, CancellationToken ct)
    {
        Task<RebalanceOutcome> waitTask;
        int generation;

        lock (_lock)
        {
            var group = GetOrAddGroup(groupId);
            
            if (!group.Members.TryGetValue(memberId, out var member) || !TopicsEqual(member.Topics, topics))
            {
                // New or changed member, but since we trigger a fresh rebalance for any Stable join,
                // we handle this uniformly below.
            }

            if (group.State == GroupState.Stable || group.State == GroupState.CompletingRebalance)
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
                 group.RebalanceTcs = new TaskCompletionSource<RebalanceOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            generation = group.Generation;
            waitTask = group.RebalanceTcs.Task;

            if (group.Members.Values.All(m => m.HasJoined))
            {
                group.State = GroupState.CompletingRebalance;
                var snapshot = group.Members.Values.Select(m => new GroupMemberSnapshot(m.MemberId, m.Topics, Array.Empty<TopicPartition>())).ToList();
                var completionOutcome = new RebalanceOutcome(group.Generation, group.LeaderId!, snapshot);
                group.RebalanceTcs.TrySetResult(completionOutcome);
            }
        }

        RebalanceOutcome outcome;
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(rebalanceTimeout);
            outcome = await waitTask.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            lock (_lock)
            {
                var group = GetOrAddGroup(groupId);
                if (group.Generation == generation && group.State == GroupState.PreparingRebalance && group.RebalanceTcs != null && !group.RebalanceTcs.Task.IsCompleted)
                {
                    var dead = group.Members.Values.Where(m => !m.HasJoined).Select(m => m.MemberId).ToList();
                    foreach (var d in dead) group.Members.Remove(d);
                    
                    if (group.LeaderId != null && dead.Contains(group.LeaderId))
                    {
                        using var enumerator = group.Members.Keys.GetEnumerator();
                        group.LeaderId = enumerator.MoveNext() ? enumerator.Current : null;
                    }

                    group.State = GroupState.CompletingRebalance;
                    var snapshot = group.Members.Values.Select(m => new GroupMemberSnapshot(m.MemberId, m.Topics, Array.Empty<TopicPartition>())).ToList();
                    var rebalanceOutcome = new RebalanceOutcome(group.Generation, group.LeaderId!, snapshot);
                    group.RebalanceTcs.TrySetResult(rebalanceOutcome);
                }
            }
            
            try
            {
                outcome = await waitTask;
            }
            catch (OperationCanceledException)
            {
                throw new InvalidOperationException(GroupMessages.RebalanceInProgress);
            }
        }

        bool isLeader = (outcome.LeaderId == memberId);
        var membersList = isLeader ? outcome.Members : Array.Empty<GroupMemberSnapshot>();

        lock (_lock)
        {
            var group = GetOrAddGroup(groupId);
            if (isLeader && (group.SyncTcs == null || group.SyncTcs.Task.IsCompleted || group.SyncTcs.Task.IsCanceled))
            {
                group.SyncTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
        
        return new GroupJoinResult(outcome.Generation, GroupState.CompletingRebalance, isLeader, membersList);
    }
#pragma warning restore S3776

    public async Task<GroupSyncResult> SyncGroupAsync(string groupId, string memberId, int generation, IReadOnlyDictionary<string, IReadOnlyList<TopicPartition>> assignments, CancellationToken ct)
    {
        Task syncTask;
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) throw new InvalidOperationException(GroupMessages.GroupNotFound);
            if (group.Generation != generation) throw new InvalidOperationException(GroupMessages.Fenced);
            
            if (group.LeaderId == memberId && group.State == GroupState.CompletingRebalance)
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
            throw new InvalidOperationException(GroupMessages.RebalanceInProgress);
        }

        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) throw new InvalidOperationException(GroupMessages.GroupNotFound);
            if (!group.Members.TryGetValue(memberId, out var member)) throw new InvalidOperationException(GroupMessages.MemberNotFound);
            if (group.Generation != generation) throw new InvalidOperationException(GroupMessages.Fenced);

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

    public Flumewright.Protocol.GroupErrorCode RecordHeartbeat(string groupId, string memberId, int expectedGeneration)
    {
        lock (_lock)
        {
            if (!_groups.TryGetValue(groupId, out var group)) return Flumewright.Protocol.GroupErrorCode.GroupUnknownMember;
            if (!group.Members.TryGetValue(memberId, out var member)) return Flumewright.Protocol.GroupErrorCode.GroupUnknownMember;
            if (group.Generation != expectedGeneration) return Flumewright.Protocol.GroupErrorCode.GroupFenced;

            member.LastHeartbeat = DateTimeOffset.UtcNow;
            if (group.State == GroupState.PreparingRebalance || group.State == GroupState.CompletingRebalance)
            {
                return Flumewright.Protocol.GroupErrorCode.GroupRebalanceInProgress;
            }
            return Flumewright.Protocol.GroupErrorCode.GroupOk;
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

    public int? GetGroupGeneration(string groupId)
    {
        if (_groups.TryGetValue(groupId, out var group))
        {
            return group.Generation;
        }
        return null;
    }

    private static bool TopicsEqual(IReadOnlyList<string> a, IReadOnlyList<string> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }

    private sealed class ConsumerGroup
    {
        public string GroupId { get; }
        private int _generation;
        public int Generation 
        { 
            get => Volatile.Read(ref _generation); 
            set => Volatile.Write(ref _generation, value); 
        }
        public GroupState State { get; set; }
        public string? LeaderId { get; set; }
        public Dictionary<string, GroupMember> Members { get; } = new();

        public TaskCompletionSource<RebalanceOutcome>? RebalanceTcs { get; set; }
        public TaskCompletionSource? SyncTcs { get; set; }

        public ConsumerGroup(string groupId)
        {
            GroupId = groupId;
            Generation = 0;
            State = GroupState.Stable;
        }
    }

    private sealed class GroupMember
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
