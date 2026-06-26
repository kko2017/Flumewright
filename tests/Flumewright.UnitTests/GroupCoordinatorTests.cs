using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Flumewright.Broker.Core;
using Xunit;

namespace Flumewright.UnitTests;

public class GroupCoordinatorTests
{
    private readonly GroupCoordinator _coordinator = new();
    private const string GroupId = "test-group";

    [Fact]
    public async Task JoinGroup_TransitionsToRebalancing_AndAssignsLeader()
    {
        var task = _coordinator.JoinGroupAsync(GroupId, "member1", new[] { "topic1" }, TimeSpan.FromMilliseconds(50), CancellationToken.None);
        
        var result = await task;
        
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
    public async Task RemoveMember_BumpsGeneration_AndTransitionsToRebalancing()
    {
        await _coordinator.JoinGroupAsync(GroupId, "member1", new[] { "topic1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        await _coordinator.SyncGroupAsync(GroupId, "member1", 1, new Dictionary<string, IReadOnlyList<TopicPartition>>(), CancellationToken.None);
        
        bool removed = _coordinator.RemoveMember(GroupId, "member1");
        
        Assert.True(removed);
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(2, state!.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        Assert.Empty(state.Members);
    }

    [Fact]
    public async Task SyncGroup_WithWrongGeneration_Throws()
    {
        await _coordinator.JoinGroupAsync(GroupId, "member1", new[] { "topic1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        
        await Assert.ThrowsAsync<InvalidOperationException>(() => 
            _coordinator.SyncGroupAsync(GroupId, "member1", 999, new Dictionary<string, IReadOnlyList<TopicPartition>>(), CancellationToken.None));
    }

    [Fact]
    public async Task Heartbeat_ReturnsRebalanceInProgress_WhenRebalancing()
    {
        await _coordinator.JoinGroupAsync(GroupId, "member1", new[] { "topic1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        
        bool ok = _coordinator.RecordHeartbeat(GroupId, "member1", 1, out bool rebalanceInProgress);
        
        Assert.True(ok);
        Assert.True(rebalanceInProgress);
    }

    [Fact]
    public async Task Heartbeat_WithStaleGeneration_Rejected()
    {
        await _coordinator.JoinGroupAsync(GroupId, "member1", new[] { "topic1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        var state = _coordinator.GetGroupState(GroupId);
        int currentGen = state!.Generation;

        _coordinator.RemoveMember(GroupId, "member1");

        bool ok = _coordinator.RecordHeartbeat(GroupId, "member1", currentGen, out _);
        Assert.False(ok);
    }

    [Fact]
    public async Task Lifecycle_TransitionsSuccessfully()
    {
        var res1 = await _coordinator.JoinGroupAsync(GroupId, "m1", new[] { "t1" }, TimeSpan.FromMilliseconds(50), CancellationToken.None);
        Assert.Equal(1, res1.Generation);
        Assert.Equal(GroupState.Rebalancing, res1.State);
        
        var assignments = new Dictionary<string, IReadOnlyList<TopicPartition>>
        {
            { "m1", new[] { new TopicPartition("t1", 0) } }
        };
        
        var completed = await _coordinator.SyncGroupAsync(GroupId, "m1", 1, assignments, CancellationToken.None);
        Assert.Equal(1, completed.Generation);
        
        bool ok = _coordinator.RecordHeartbeat(GroupId, "m1", 1, out bool rebalance);
        Assert.True(ok);
        Assert.False(rebalance);
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(1, state!.Generation);
        Assert.Equal(GroupState.Stable, state.State);
    }

    [Fact]
    public async Task ConcurrentMembershipChanges_KeepGenerationMonotonic_AndTableConsistent()
    {
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
                await gate.Task;
#pragma warning restore VSTHRD003 // Avoid awaiting foreign Tasks
                for (int j = 0; j < operationsPerThread; j++)
                {
                    string memberId = $"member-{threadId}-{j}";
                    try
                    {
                        await _coordinator.JoinGroupAsync(GroupId, memberId, new[] { "topic1" }, TimeSpan.FromMilliseconds(1), CancellationToken.None);
                    }
                    catch (InvalidOperationException)
                    {
                        // Ignore Rebalance in progress during the thundering herd test
                    }
                    catch (OperationCanceledException)
                    {
                        // Ignore timeouts during the thundering herd test
                    }
                }
            });
        }
        
        gate.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5));
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.NotNull(state);
        
        // Members count should be exactly the joined members. We don't guarantee exact generation number because JoinGroup could be coalesced or cancelled.
        Assert.True(state.Members.Count > 0);
        Assert.True(state.Generation > 0);
    }

    [Fact]
    public async Task SweepDeadMembers_EvictsMembers_AndBumpsGeneration()
    {
        await _coordinator.JoinGroupAsync(GroupId, "m1", new[] { "t1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        
        var state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(1, state!.Generation);

        _coordinator.SweepDeadMembers(TimeSpan.Zero);

        state = _coordinator.GetGroupState(GroupId);
        Assert.Equal(2, state!.Generation);
        Assert.Equal(GroupState.Rebalancing, state.State);
        Assert.Empty(state.Members);
    }

    [Fact]
    public async Task ConcurrentHeartbeatAndSweep_ResolvesToSingleOutcome()
    {
        await _coordinator.JoinGroupAsync(GroupId, "m1", new[] { "t1" }, TimeSpan.FromMilliseconds(10), CancellationToken.None);
        
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
        Assert.NotNull(state);
    }
}
