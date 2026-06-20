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
    private sealed class Partition
    {
        public int Index { get; }
        private readonly List<StoredMessage> _messages = new();
        private readonly object _lock = new();
        private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Partition(int index)
        {
            Index = index;
        }

        public (int Partition, long Offset) Append(
            IReadOnlyDictionary<string, string> headers,
            ReadOnlyMemory<byte> payload)
        {
            TaskCompletionSource oldTcs;
            long offset;
            lock (_lock)
            {
                offset = _messages.Count;
                var message = new StoredMessage(Index, offset, headers, payload);
                _messages.Add(message);
                oldTcs = _tcs;
                _tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            oldTcs.TrySetResult();
            return (Index, offset);
        }

        public async IAsyncEnumerable<StoredMessage> ReadFromOffsetAsync(
            long startOffset,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            long currentOffset = startOffset;
            while (!ct.IsCancellationRequested)
            {
                StoredMessage? msg = null;
                Task? waitTask = null;

                lock (_lock)
                {
                    if (currentOffset < _messages.Count)
                    {
                        msg = _messages[(int)currentOffset];
                    }
                    else
                    {
                        waitTask = _tcs.Task;
                    }
                }

                if (msg != null)
                {
                    yield return msg;
                    currentOffset++;
                }
                else if (waitTask != null)
                {
                    await waitTask.WaitAsync(ct);
                }
            }
        }

        public int MessageCount
        {
            get
            {
                lock (_lock)
                {
                    return _messages.Count;
                }
            }
        }
    }

    private sealed class Topic
    {
        public Partition[] Partitions { get; }
        private long _rrCounter = -1;

        public Topic(int partitionCount)
        {
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

    public InMemoryTopicStore() : this(4)
    {
    }

    public InMemoryTopicStore(int defaultPartitionCount)
    {
        _defaultPartitionCount = defaultPartitionCount;
    }

    public InMemoryTopicStore(IConfiguration configuration)
    {
        _defaultPartitionCount = configuration.GetValue<int>("Broker:PartitionsPerTopic", 4);
    }

    public ValueTask<(int Partition, long Offset)> PublishAsync(
        string topic,
        ReadOnlyMemory<byte> partitionKey,
        IReadOnlyDictionary<string, string> headers,
        ReadOnlyMemory<byte> payload,
        CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic(_defaultPartitionCount));

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
        var result = partition.Append(headers, payload);

        return new ValueTask<(int Partition, long Offset)>(result);
    }

    public IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        CancellationToken ct = default)
    {
        return SubscribeAsync(topic, -1, ct);
    }

    public async IAsyncEnumerable<StoredMessage> SubscribeAsync(
        string topic,
        long startOffset,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic(_defaultPartitionCount));

        var channel = Channel.CreateUnbounded<StoredMessage>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true
        });

        var partitionTasks = new List<Task>();

        for (int i = 0; i < topicState.Partitions.Length; i++)
        {
            int partitionIndex = i;
            var partition = topicState.Partitions[partitionIndex];

            long initialOffset = startOffset;
            if (initialOffset < 0)
            {
                initialOffset = partition.MessageCount;
            }

            var task = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in partition.ReadFromOffsetAsync(initialOffset, ct))
                    {
                        await channel.Writer.WriteAsync(msg, ct);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Cancellation is normal shutdown behavior for this partition reader task.
                }
            }, CancellationToken.None);

            partitionTasks.Add(task);
        }

        _ = Task.WhenAll(partitionTasks).ContinueWith(t => channel.Writer.TryComplete(t.Exception), TaskScheduler.Default);

        while (await channel.Reader.WaitToReadAsync(ct))
        {
            while (channel.Reader.TryRead(out var msg))
            {
                yield return msg;
            }
        }
    }

    public IAsyncEnumerable<StoredMessage> ReadPartitionAsync(
        string topic,
        int partition,
        long startOffset,
        CancellationToken ct = default)
    {
        var topicState = _topics.GetOrAdd(topic, _ => new Topic(_defaultPartitionCount));
        if (partition < 0 || partition >= topicState.Partitions.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(partition), $"Partition must be between 0 and {topicState.Partitions.Length - 1}");
        }

        return topicState.Partitions[partition].ReadFromOffsetAsync(startOffset, ct);
    }
}
