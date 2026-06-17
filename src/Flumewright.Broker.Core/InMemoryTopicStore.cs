using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace Flumewright.Broker.Core;

public sealed class InMemoryTopicStore : ITopicStore
{
    private class Partition
    {
        public int Index { get; }
        public ConcurrentDictionary<Guid, Channel<StoredMessage>> Subscribers { get; } = new();
        private long _offsetCounter = -1;
        private long _droppedCount = 0;

        public Partition(int index)
        {
            Index = index;
        }

        public long GetNextOffset() => Interlocked.Increment(ref _offsetCounter);
        public void IncrementDroppedCount() => Interlocked.Increment(ref _droppedCount);
        public long DroppedCount => Volatile.Read(ref _droppedCount);
    }

    private class Topic
    {
        public Partition[] Partitions { get; }
        private long _rrCounter = -1;
        public int ChannelCapacity { get; }

        public Topic(int partitionCount, int channelCapacity)
        {
            ChannelCapacity = channelCapacity;
            Partitions = new Partition[partitionCount];
            for (int i = 0; i < partitionCount; i++)
            {
                Partitions[i] = new Partition(i);
            }
        }

        public long GetNextRoundRobin() => Interlocked.Increment(ref _rrCounter);
    }

    private readonly ConcurrentDictionary<string, Topic> _topics = new();
    private readonly int _defaultPartitionCount;
    private readonly int _channelCapacity;

    public InMemoryTopicStore() : this(4, 10000)
    {
    }

    public InMemoryTopicStore(int defaultPartitionCount, int channelCapacity)
    {
        _defaultPartitionCount = defaultPartitionCount;
        _channelCapacity = channelCapacity;
    }

    public InMemoryTopicStore(IConfiguration configuration)
    {
        _defaultPartitionCount = configuration.GetValue<int>("Broker:PartitionsPerTopic", 4);
        _channelCapacity = configuration.GetValue<int>("Broker:ChannelCapacityPerPartition", 10000);
    }

    public ValueTask<(int Partition, long Offset)> PublishAsync(
        string topic,
        ReadOnlyMemory<byte> partitionKey,
        IReadOnlyDictionary<string, string> headers,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic(_defaultPartitionCount, _channelCapacity));
        
        int partitionIndex;
        if (partitionKey.IsEmpty)
        {
            long rr = topicState.GetNextRoundRobin();
            partitionIndex = PartitionRouter.RoundRobin(rr, topicState.Partitions.Length);
        }
        else
        {
            partitionIndex = PartitionRouter.ForKey(partitionKey.Span, topicState.Partitions.Length);
        }

        var partition = topicState.Partitions[partitionIndex];
        long offset = partition.GetNextOffset();
        var message = new StoredMessage(partitionIndex, offset, headers, payload);

        foreach (var sub in partition.Subscribers.Values)
        {
            bool success = sub.Writer.TryWrite(message);
            if (!success)
            {
                partition.IncrementDroppedCount();
                // M5: real backpressure
            }
        }

        return new ValueTask<(int Partition, long Offset)>((partitionIndex, offset));
    }

    public async IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic(_defaultPartitionCount, _channelCapacity));
        var subId = Guid.NewGuid();

        var partitionChannels = new Channel<StoredMessage>[topicState.Partitions.Length];
        for (int i = 0; i < topicState.Partitions.Length; i++)
        {
            var channel = Channel.CreateBounded<StoredMessage>(new BoundedChannelOptions(topicState.ChannelCapacity)
            {
                SingleWriter = false,
                SingleReader = true,
                FullMode = BoundedChannelFullMode.DropWrite
            });
            partitionChannels[i] = channel;
            topicState.Partitions[i].Subscribers.TryAdd(subId, channel);
        }

        var mergedChannel = Channel.CreateUnbounded<StoredMessage>();
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var mergeTasks = new List<Task>();

        for (int i = 0; i < topicState.Partitions.Length; i++)
        {
            var reader = partitionChannels[i].Reader;
            mergeTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in reader.ReadAllAsync(cts.Token))
                    {
                        await mergedChannel.Writer.WriteAsync(msg, cts.Token);
                    }
                }
                catch (OperationCanceledException) {}
            }));
        }

        try
        {
            await foreach (var msg in mergedChannel.Reader.ReadAllAsync(ct))
            {
                yield return msg;
            }
        }
        finally
        {
            for (int i = 0; i < topicState.Partitions.Length; i++)
            {
                if (topicState.Partitions[i].Subscribers.TryRemove(subId, out var channel))
                {
                    channel.Writer.Complete();
                }
            }

            cts.Cancel();
            try
            {
                await Task.WhenAll(mergeTasks).ConfigureAwait(false);
            }
            catch { /* Ignore cancellation errors in task cleanup */ }

            mergedChannel.Writer.Complete();
        }
    }

    internal long GetDroppedCount(string topic, int partitionIndex)
    {
        if (_topics.TryGetValue(topic, out var topicState) && partitionIndex >= 0 && partitionIndex < topicState.Partitions.Length)
        {
            return topicState.Partitions[partitionIndex].DroppedCount;
        }
        return 0;
    }
}
