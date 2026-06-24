using System;

namespace Flumewright.Client.Resilience;

/// <summary>
/// Default non-blocking retry policy.
/// Routes retries to "{topic}.retry" immediately (zero delay).
/// By default, all exceptions are considered transient except <see cref="PoisonMessageException"/>.
/// </summary>
public class DefaultRetryPolicy : IRetryPolicy
{
    public int MaxAttempts { get; }

    public DefaultRetryPolicy(int maxAttempts = 3)
    {
        MaxAttempts = maxAttempts;
    }

    public bool ShouldRetry(Exception failure)
    {
        // Default split: if it's explicitly marked as poison, do not retry.
        // Otherwise assume transient.
        if (failure is PoisonMessageException || failure?.InnerException is PoisonMessageException)
        {
            return false;
        }
        return true;
    }

    public RetryAction GetNextAttemptAction(string originalTopic, int attemptCount)
    {
        // Phase 1: Fixed destination and zero delay.
        return new RetryAction($"{originalTopic}.retry", TimeSpan.Zero);
    }
}
