using System.Collections.Generic;
using System.IO;
using System;
using FluentAssertions;
using Flumewright.Broker.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Flumewright.UnitTests;

public sealed class MtlsConfigTests : IDisposable
{
    private readonly string _tempServerCertPath;
    private readonly string _tempCaCertPath;

    public MtlsConfigTests()
    {
        _tempServerCertPath = Path.GetTempFileName();
        _tempCaCertPath = Path.GetTempFileName();
    }

    public void Dispose()
    {
        if (File.Exists(_tempServerCertPath)) File.Delete(_tempServerCertPath);
        if (File.Exists(_tempCaCertPath)) File.Delete(_tempCaCertPath);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FromConfiguration_Default_MtlsIsOff()
    {
        var config = new ConfigurationBuilder().Build();
        var mtlsConfig = MtlsConfig.FromConfiguration(config);

        mtlsConfig.RequireClientCertificate.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FromConfiguration_MtlsEnabledWithValidPaths_ReturnsConfig()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Broker:RequireClientCertificate"] = "true",
                ["Broker:ServerCertPath"] = _tempServerCertPath,
                ["Broker:CaCertPath"] = _tempCaCertPath
            })
            .Build();

        var mtlsConfig = MtlsConfig.FromConfiguration(config);

        mtlsConfig.RequireClientCertificate.Should().BeTrue();
        mtlsConfig.ServerCertPath.Should().Be(_tempServerCertPath);
        mtlsConfig.CaCertPath.Should().Be(_tempCaCertPath);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FromConfiguration_MtlsEnabledButServerCertMissing_Throws()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Broker:RequireClientCertificate"] = "true",
                ["Broker:ServerCertPath"] = "non-existent-file.pfx",
                ["Broker:CaCertPath"] = _tempCaCertPath
            })
            .Build();

        var act = () => MtlsConfig.FromConfiguration(config);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*ServerCertPath 'non-existent-file.pfx' is missing or invalid*");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void FromConfiguration_MtlsEnabledButCaCertMissing_Throws()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Broker:RequireClientCertificate"] = "true",
                ["Broker:ServerCertPath"] = _tempServerCertPath,
                ["Broker:CaCertPath"] = "non-existent-file.crt"
            })
            .Build();

        var act = () => MtlsConfig.FromConfiguration(config);

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*CaCertPath 'non-existent-file.crt' is missing or invalid*");
    }
}
