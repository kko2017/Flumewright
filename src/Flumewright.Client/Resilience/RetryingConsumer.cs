using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Flumewright.Client.Resilience;

/// <summary>
/// A non-blocking retry helper that composes core SDK primitives.
/// Subscribes to a group, invokes a handler, and on failure publishes the message 
/// to a retry or DLQ topic (with headers) before committing the original offset.
/// </summary>
public sealed class RetryingConsumer
{
    private readonly FlumewrightSubscriber _subscriber;
    private readonly FlumewrightPublisher _publisher;
    private readonly IRetryPolicy _policy;

    public RetryingConsumer(FlumewrightSubscriber subscriber, FlumewrightPublisher publisher, IRetryPolicy policy)
    {
        _subscriber = subscriber;
        _publisher = publisher;
        _policy = policy;
    }

    public async Task ConsumeGroupAsync(
        string topic,
        string groupId,
        IReadOnlyList<int> partitions,
        Func<ReceivedMessage, CancellationToken, Task> messageHandler,
        FlumewrightOffsetReset reset = FlumewrightOffsetReset.Earliest,
        CancellationToken ct = default)
    {
        await foreach (var msg in _subscriber.SubscribeGroupAsync(topic, groupId, partitions, reset, ct))
        {
            try
            {
                await messageHandler(msg, ct);
                
                // On success, commit the offset
                await _subscriber.CommitOffsetAsync(groupId, topic, msg.Partition, msg.Offset, ct);
            }
#pragma warning disable CA1031 // We intentionally catch all handler exceptions to apply the retry policy
            catch (Exception ex)
#pragma warning restore CA1031
            {
                // On failure, apply the policy (non-blocking retry)
                await HandleFailureAsync(msg, ex, groupId, topic, ct);
            }
        }
    }

    private async Task HandleFailureAsync(
        ReceivedMessage originalMsg, 
        Exception failure, 
        string groupId, 
        string consumedTopic, 
        CancellationToken ct)
    {
        int attempts = 1;
        if (originalMsg.Headers.TryGetValue("x-attempts", out var attemptsStr) && int.TryParse(attemptsStr, out var parsed))
        {
            attempts = parsed + 1;
        }

        string trueOriginalTopic = originalMsg.Headers.TryGetValue("x-original-topic", out var orig) ? orig : consumedTopic;

        bool shouldRetry = _policy.ShouldRetry(failure) && attempts <= _policy.MaxAttempts;
        string destinationTopic;

        if (shouldRetry)
        {
            var action = _policy.GetNextAttemptAction(trueOriginalTopic, attempts);
            destinationTopic = action.DestinationTopic;
            // Note: Phase-1 delay is intentionally ignored here.
            // No Task.Delay is used for synchronization.
        }
        else
        {
            // Send directly to DLQ
            destinationTopic = $"{trueOriginalTopic}.dlq";
        }

        // Prepare headers for the OUTGOING copy (never mutate the stored message in-place)
        var newHeaders = new Dictionary<string, string>(originalMsg.Headers)
        {
            ["x-attempts"] = attempts.ToString(),
            ["x-failure-reason"] = failure.GetType().Name
        };

        // Set original metadata only on the first move
        if (!newHeaders.ContainsKey("x-original-topic"))
        {
            newHeaders["x-original-topic"] = originalMsg.Topic;
            newHeaders["x-original-partition"] = originalMsg.Partition.ToString();
            newHeaders["x-original-offset"] = originalMsg.Offset.ToString();
        }

        // 1. Publish the outgoing copy to the retry/dlq topic
        await _publisher.PublishAsync(destinationTopic, originalMsg.Payload, null, newHeaders, null, ct);

        // 2. Commit the original offset so the original partition can progress.
        // This publish-then-commit boundary is explicitly non-atomic (at-least-once duplication accepted).
        await _subscriber.CommitOffsetAsync(groupId, consumedTopic, originalMsg.Partition, originalMsg.Offset, ct);
    }
}
