using System;

namespace Flumewright.Client.Resilience;

/// <summary>
/// The outcome of a retry policy evaluation: destination topic and backoff delay.
/// </summary>
public record RetryAction(string DestinationTopic, TimeSpan Delay);
