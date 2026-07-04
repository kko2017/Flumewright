using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Flumewright.CertGen;
using Flumewright.Security.Cryptography;
using Xunit;

namespace Flumewright.UnitTests;

public class ClientCertificateValidatorTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void Validate_WithValidClientCert_ReturnsTrue()
    {
        using var caCert = CertificateGenerator.CreateCaCertificate("CN=Test CA");
        using var clientCert = CertificateGenerator.CreateClientCertificate(caCert, "alice");

        var result = ClientCertificateValidator.Validate(clientCert, caCert);

        result.Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Validate_WithNullCert_ReturnsFalse()
    {
        using var caCert = CertificateGenerator.CreateCaCertificate("CN=Test CA");

        var result = ClientCertificateValidator.Validate(null, caCert);

        result.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Validate_WithUntrustedCert_ReturnsFalse()
    {
        using var caCert = CertificateGenerator.CreateCaCertificate("CN=Test CA");

        using var rogueCaCert = CertificateGenerator.CreateCaCertificate("CN=Rogue CA");
        using var clientCert = CertificateGenerator.CreateClientCertificate(rogueCaCert, "alice");

        var result = ClientCertificateValidator.Validate(clientCert, caCert);

        result.Should().BeFalse();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Validate_WithoutClientAuthEku_ReturnsFalse()
    {
        using var caCert = CertificateGenerator.CreateCaCertificate("CN=Test CA");
        // Broker cert uses serverAuth EKU, not clientAuth
        using var brokerCert = CertificateGenerator.CreateBrokerCertificate(caCert);

        var result = ClientCertificateValidator.Validate(brokerCert, caCert);

        result.Should().BeFalse();
    }
}
