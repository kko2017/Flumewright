using System;

namespace Flumewright.Client.Resilience;

/// <summary>
/// A policy that dictates how message processing failures are handled.
/// Implementations should be thread-safe value objects or pure functions.
/// </summary>
public interface IRetryPolicy
{
    /// <summary>
    /// The maximum number of retry attempts before a message is sent to the Dead-Letter Queue (DLQ).
    /// </summary>
    int MaxAttempts { get; }

    /// <summary>
    /// Determines whether the given exception represents a transient failure (which should be retried)
    /// or a poison pill (which should go straight to the DLQ).
    /// </summary>
    bool ShouldRetry(Exception failure);

    /// <summary>
    /// Determines where to send the message for the next attempt, and how long to delay.
    /// In Phase 1, the delay will be ignored, but the shape supports future delayed backoff.
    /// </summary>
    RetryAction GetNextAttemptAction(string originalTopic, int attemptCount);
}
