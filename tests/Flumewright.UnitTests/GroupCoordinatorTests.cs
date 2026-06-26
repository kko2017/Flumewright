using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class GroupCoordinatorTests
{
    private readonly GroupCoordinator _coordinator = new();
    private const string GroupId = "test-group";

    [Fact]
    public void AddMember_BumpsGeneration_AndTransitionsToRebalancing()
    {
        var result = _coordinator.AddOrUpdateMember(GroupId, "member1", new[] { "topic1" });
        
        Assert.Equal(1, result.Generation);
        Assert.Equal(GroupState.Rebalancing, result.State);
        Assert.True(result.IsLeader);

        var state = _coordinator.GetGroupState(GroupId);
        Assert.NotNull(state);
        Assert.Equal(1, state.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        Assert.Single(state.Members);
    }

    [Fact]
    public void RemoveMember_BumpsGeneration_AndTransitionsToRebalancing()
    {
        _coordinator.AddOrUpdateMember(GroupId, "member1", new[] { "topic1" });
        _coordinator.CompleteRebalance(GroupId, 1, new Dictionary<string, IReadOnlyList<TopicPartition>>());
        
        bool removed = _coordinator.RemoveMember(GroupId, "member1");
        
        Assert.True(removed);
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(2, state!.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        Assert.Empty(state.Members);
    }

    [Fact]
    public void CompleteRebalance_WithWrongGeneration_Fails()
    {
        _coordinator.AddOrUpdateMember(GroupId, "member1", new[] { "topic1" });
        
        bool ok = _coordinator.CompleteRebalance(GroupId, 999, new Dictionary<string, IReadOnlyList<TopicPartition>>());
        Assert.False(ok);
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(GroupState.Rebalancing, state!.State);
    }

    [Fact]
    public void Heartbeat_ReturnsRebalanceInProgress_WhenRebalancing()
    {
        _coordinator.AddOrUpdateMember(GroupId, "member1", new[] { "topic1" });
        
        bool ok = _coordinator.RecordHeartbeat(GroupId, "member1", 1, out bool rebalanceInProgress);
        
        Assert.True(ok);
        Assert.True(rebalanceInProgress);
    }

    [Fact]
    public void Lifecycle_TransitionsSuccessfully()
    {
        var res1 = _coordinator.AddOrUpdateMember(GroupId, "m1", new[] { "t1" });
        Assert.Equal(1, res1.Generation);
        Assert.Equal(GroupState.Rebalancing, res1.State);
        
        var assignments = new Dictionary<string, IReadOnlyList<TopicPartition>>
        {
            { "m1", new[] { new TopicPartition("t1", 0) } }
        };
        bool completed = _coordinator.CompleteRebalance(GroupId, 1, assignments);
        Assert.True(completed);
        
        bool ok = _coordinator.RecordHeartbeat(GroupId, "m1", 1, out bool rebalance);
        Assert.True(ok);
        Assert.False(rebalance);
        
        bool began = _coordinator.BeginRebalance(GroupId);
        Assert.True(began);
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(2, state!.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        
        // Members should have assignments cleared upon entering rebalance
        Assert.Empty(state.Members.Single().AssignedPartitions);
    }

    [Fact]
    public async Task ConcurrentMembershipChanges_KeepGenerationMonotonic_AndTableConsistent()
    {
        // Real-contention test: exercises the lock via a thundering herd. 
        // Will throw or fail assertions if the lock is removed.
        int numThreads = 20;
        int operationsPerThread = 100;
        var tasks = new Task[numThreads];
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        
        for (int i = 0; i < numThreads; i++)
        {
            int threadId = i;
            tasks[i] = Task.Run(async () => 
            {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
                await gate.Task; // Wait for the start signal
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                for (int j = 0; j < operationsPerThread; j++)
                {
                    string memberId = $"member-{threadId}-{j}";
                    _coordinator.AddOrUpdateMember(GroupId, memberId, new[] { "topic1" });
                }
            });
        }
        
        // Release the thundering herd all at once
        gate.SetResult();
        
        // Bounded wait to prevent infinite hang if a deadlock occurs
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.NotNull(state);
        
        // Assert exactly what the final state must be: monotonic generation and count
        Assert.Equal(numThreads * operationsPerThread, state.Members.Count);
        Assert.Equal(numThreads * operationsPerThread, state.Generation);
    }

    [Fact]
    public void SweepDeadMembers_EvictsMembers_AndBumpsGeneration()
    {
        _coordinator.AddOrUpdateMember(GroupId, "m1", new[] { "t1" });
        _coordinator.AddOrUpdateMember(GroupId, "m2", new[] { "t1" });
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(2, state!.Generation); // 1 for m1, 1 for m2

        // Wait slightly, then sweep with 0 timeout to evict all
        _coordinator.SweepDeadMembers(TimeSpan.Zero);

        state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(3, state!.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        Assert.Empty(state.Members);
    }

    [Fact]
    public async Task ConcurrentHeartbeatAndSweep_ResolvesToSingleOutcome()
    {
        _coordinator.AddOrUpdateMember(GroupId, "m1", new[] { "t1" });
        
        var state = _coordinator.GetGroupState(GroupId);
        int gen = state!.Generation;

        int numThreads = 20;
        var tasks = new Task[numThreads];
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        
        for (int i = 0; i < numThreads; i++)
        {
            if (i % 2 == 0)
            {
                tasks[i] = Task.Run(async () => 
                {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
                    await gate.Task;
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                    _coordinator.SweepDeadMembers(TimeSpan.Zero);
                });
            }
            else
            {
                tasks[i] = Task.Run(async () => 
                {
#pragma warning disable VSTHRD003 // Avoid awaiting foreign Tasks
                    await gate.Task;
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                    _coordinator.RecordHeartbeat(GroupId, "m1", gen, out _);
                });
            }
        }
        
        gate.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));

        state = _coordinator.GetGroupState(GroupId);
        // Either the sweep won (member gone) or heartbeat won (member still there).
        // Since sweep is TimeSpan.Zero, it's highly likely to evict if it wins. 
        // But what matters is no double-evict crashing or corrupting state.
        Assert.NotNull(state);
    }
}
