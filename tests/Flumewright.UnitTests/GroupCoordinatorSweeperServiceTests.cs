using FluentAssertions;
using Flumewright.Broker.Services;
using Xunit;
using System;

namespace Flumewright.UnitTests;

public class GroupCoordinatorSweeperServiceTests
{
    [Theory]
    [InlineData(-1.0, 10.0, 10.0)]
    [InlineData(0.0, 10.0, 10.0)]
    [InlineData(5.0, 10.0, 5.0)]
    [InlineData(0.0, 2.0, 2.0)]
    [InlineData(0.25, 2.0, 0.25)]
    public void ResolveDuration_ReturnsExpectedTimeSpan(double configured, double defaultValue, double expected)
    {
        var result = GroupCoordinatorSweeperService.ResolveDuration(configured, defaultValue);
        result.Should().Be(TimeSpan.FromSeconds(expected));
    }
}
