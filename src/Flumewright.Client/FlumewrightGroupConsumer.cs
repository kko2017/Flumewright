using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Flumewright.Protocol;
using Grpc.Core;
using Grpc.Net.Client;

namespace Flumewright.Client;

internal enum ClientGroupErrorCode
{
    Ok = 0,
    Fenced = 1,
    RebalanceInProgress = 2,
    UnknownMember = 3
}

internal static class GroupErrorCodeExtensions
{
    public static ClientGroupErrorCode ToClientCode(this Protocol.GroupErrorCode code)
    {
        return code switch
        {
            Protocol.GroupErrorCode.GroupOk => ClientGroupErrorCode.Ok,
            Protocol.GroupErrorCode.GroupFenced => ClientGroupErrorCode.Fenced,
            Protocol.GroupErrorCode.GroupRebalanceInProgress => ClientGroupErrorCode.RebalanceInProgress,
            Protocol.GroupErrorCode.GroupUnknownMember => ClientGroupErrorCode.UnknownMember,
            _ => throw new ArgumentOutOfRangeException(nameof(code), $"Unknown group error code: {code}")
        };
    }
}

public sealed class FlumewrightGroupConsumer : IDisposable
{
    private readonly GrpcChannel? _channel;
    private readonly MessageBus.MessageBusClient _client;
    private readonly string _groupId;
    private readonly string _memberId;
    
    private bool? _isDynamic;
    private readonly object _lock = new();
    private volatile int _currentGeneration;

    public FlumewrightGroupConsumer(string address, string groupId, string? memberId = null)
    {
        _channel = GrpcChannel.ForAddress(address);
        _client = new MessageBus.MessageBusClient(_channel);
        _groupId = groupId;
        _memberId = memberId ?? Guid.NewGuid().ToString();
    }

    internal FlumewrightGroupConsumer(MessageBus.MessageBusClient client, string groupId, string? memberId = null)
    {
        _client = client;
        _groupId = groupId;
        _memberId = memberId ?? Guid.NewGuid().ToString();
    }

    public async IAsyncEnumerable<ReceivedMessage> AssignAsync(
        string topic,
        IReadOnlyList<int> partitions,
        FlumewrightOffsetReset reset = FlumewrightOffsetReset.Earliest,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (_isDynamic == true)
                throw new InvalidOperationException("Static assignment and dynamic group membership cannot be mixed on the same consumer.");
            _isDynamic = false;
        }

        var req = new SubscribeRequest 
        { 
            Topic = topic,
            GroupId = _groupId,
            Reset = reset == FlumewrightOffsetReset.Earliest ? OffsetReset.Earliest : OffsetReset.Latest
        };
        req.Partitions.AddRange(partitions);

        using var call = _client.Subscribe(req, cancellationToken: ct);
        await foreach (var d in call.ResponseStream.ReadAllAsync(ct))
        {
            yield return new ReceivedMessage(d.Topic, d.Offset, new Dictionary<string, string>(d.Headers), d.Payload.ToByteArray(), d.Partition);
        }
    }

    public async IAsyncEnumerable<ReceivedMessage> SubscribeAsync(
        IReadOnlyList<string> topics,
        IReadOnlyDictionary<string, int> partitionCounts,
        IAssignmentStrategy strategy,
        FlumewrightOffsetReset reset = FlumewrightOffsetReset.Earliest,
        TimeSpan? heartbeatInterval = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        lock (_lock)
        {
            if (_isDynamic == false)
                throw new InvalidOperationException("Static assignment and dynamic group membership cannot be mixed on the same consumer.");
            _isDynamic = true;
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var loopCt = cts.Token;
        
        while (!loopCt.IsCancellationRequested)
        {
            var enumerator = RunSingleGenerationAsync(topics, partitionCounts, strategy, reset, heartbeatInterval, loopCt)
                .GetAsyncEnumerator(loopCt);
            
            bool hasNext = false;
            try
            {
                while (true)
                {
                    try
                    {
                        hasNext = await enumerator.MoveNextAsync();
                    }
                    catch (OperationCanceledException)
                    {
                        // cancelled for rejoin or outer cancellation
                        break;
                    }
                    catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
                    {
                        // Grpc throws RpcException(Cancelled) when the CancellationToken is cancelled
                        break;
                    }
                    
                    if (!hasNext) break;
                    
                    yield return enumerator.Current;
                }
            }
            finally
            {
                await enumerator.DisposeAsync();
            }
        }
    }

    private async IAsyncEnumerable<ReceivedMessage> RunSingleGenerationAsync(
        IReadOnlyList<string> topics,
        IReadOnlyDictionary<string, int> partitionCounts,
        IAssignmentStrategy strategy,
        FlumewrightOffsetReset reset,
        TimeSpan? heartbeatInterval,
        [EnumeratorCancellation] CancellationToken loopCt)
    {
        _currentGeneration = 0;
        IReadOnlyList<MemberAssignment>? finalAssignment = null;

        // 1. JoinGroup
        var joinReq = new JoinGroupRequest
        {
            GroupId = _groupId,
            MemberId = _memberId
        };
        joinReq.Topics.AddRange(topics);

        var joinRes = await _client.JoinGroupAsync(joinReq, cancellationToken: loopCt);
        if (!joinRes.Ok)
        {
            throw new Exception($"JoinGroup failed: {joinRes.Reason}");
        }

        _currentGeneration = joinRes.Generation;
        var syncReq = new SyncGroupRequest
        {
            GroupId = _groupId,
            MemberId = _memberId,
            Generation = _currentGeneration
        };

        // 2. Leader computes assignment
        if (joinRes.IsLeader)
        {
            var assignments = strategy.Assign(joinRes.Members, partitionCounts);
            syncReq.Assignments.AddRange(assignments);
        }

        // 3. SyncGroup
        var syncRes = await _client.SyncGroupAsync(syncReq, cancellationToken: loopCt);
        if (!syncRes.Ok)
        {
            // Rebalance in progress or fenced during sync -> rejoin
            yield break;
        }

        finalAssignment = syncRes.Assignments;
        if (syncRes.Generation != _currentGeneration)
        {
            _currentGeneration = syncRes.Generation;
        }

        // 4. Start Heartbeat loop
        Task? heartbeatTask = null;
        using var heartbeatCts = CancellationTokenSource.CreateLinkedTokenSource(loopCt);
        var hbToken = heartbeatCts.Token;
        
        try
        {
            heartbeatTask = Task.Run(async () =>
            {
                try
                {
                    using var timer = new PeriodicTimer(heartbeatInterval ?? TimeSpan.FromSeconds(3));
                    while (await timer.WaitForNextTickAsync(hbToken))
                    {
                        var hbReq = new HeartbeatRequest
                        {
                            GroupId = _groupId,
                            MemberId = _memberId,
                            Generation = _currentGeneration
                        };
                        var hbRes = await _client.HeartbeatAsync(hbReq, cancellationToken: hbToken);
                        if (!hbRes.Ok)
                        {
                            var clientCode = hbRes.Code.ToClientCode();
                            if (clientCode == ClientGroupErrorCode.RebalanceInProgress || clientCode == ClientGroupErrorCode.Fenced)
                            {
                                // Cancel consumption to rejoin
                                await heartbeatCts.CancelAsync();
                                break;
                            }
                            else
                            {
                                throw new Exception($"Heartbeat failed: {hbRes.Reason} (Code: {clientCode})");
                            }
                        }
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Fault occurred: cancel foreground consumption to unblock it and propagate the fault
                    await heartbeatCts.CancelAsync();
                    throw;
                }
            }, CancellationToken.None);

            // 5. Consume assigned partitions
            var myAssignment = finalAssignment?.FirstOrDefault(a => a.MemberId == _memberId);
            if (myAssignment != null && myAssignment.Partitions.Count > 0)
            {
                var subscribeReq = new SubscribeRequest
                {
                    Topic = myAssignment.Topic,
                    GroupId = _groupId,
                    Reset = reset == FlumewrightOffsetReset.Earliest ? OffsetReset.Earliest : OffsetReset.Latest
                };
                subscribeReq.Partitions.AddRange(myAssignment.Partitions);

                using var call = _client.Subscribe(subscribeReq, cancellationToken: hbToken);
                await foreach (var d in call.ResponseStream.ReadAllAsync(hbToken))
                {
                    yield return new ReceivedMessage(d.Topic, d.Offset, new Dictionary<string, string>(d.Headers), d.Payload.ToByteArray(), d.Partition);
                }
            }
            else
            {
                // No partitions assigned, just wait for rebalance or cancellation
                await heartbeatTask;
            }
        }
        finally
        {
            if (!heartbeatCts.IsCancellationRequested)
            {
                await heartbeatCts.CancelAsync();
            }

            if (heartbeatTask != null)
            {
                try
                {
                    await heartbeatTask;
                }
                catch (OperationCanceledException) { }
            }
        }
    }

    public async Task CommitOffsetAsync(string topic, int partition, long offset, CancellationToken ct = default)
    {
        var req = new CommitRequest
        {
            GroupId = _groupId,
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Generation = _currentGeneration
        };

        var ack = await _client.CommitOffsetAsync(req, cancellationToken: ct);
        if (!ack.Ok)
        {
            throw new CommitRejectedException(ack.Reason);
        }
    }

    public async Task LeaveGroupAsync(CancellationToken ct = default)
    {
        var req = new LeaveGroupRequest
        {
            GroupId = _groupId,
            MemberId = _memberId
        };
        await _client.LeaveGroupAsync(req, cancellationToken: ct);
    }

    public void Dispose()
    {
        // Try leave group best-effort
        try
        {
            _client.LeaveGroup(new LeaveGroupRequest { GroupId = _groupId, MemberId = _memberId });
        }
        catch (RpcException) { }
        
        _channel?.Dispose();
    }
}
