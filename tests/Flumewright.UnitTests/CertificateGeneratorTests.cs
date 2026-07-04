using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using FluentAssertions;
using Flumewright.CertGen;
using Xunit;

namespace Flumewright.UnitTests;

/// <summary>
/// Verifies the certificate artifacts produced by <see cref="CertificateGenerator"/>.
/// These are pure property checks on the generated certificates — no broker involved.
/// </summary>
public sealed class CertificateGeneratorTests : IDisposable
{
    private static readonly string[] ClientNames = ["test-publisher", "test-subscriber"];

    private readonly GeneratedCertificates _certs;
    private readonly string _outputDir;

    public CertificateGeneratorTests()
    {
        _outputDir = Path.Combine(Path.GetTempPath(), $"fw-certgen-test-{Guid.NewGuid():N}");
        _certs = CertificateGenerator.GenerateAll(
            _outputDir,
            ClientNames);
    }

    public void Dispose()
    {
        _certs.Dispose();
        if (Directory.Exists(_outputDir))
        {
            Directory.Delete(_outputDir, recursive: true);
        }
    }

    // ── CA certificate checks ──

    [Fact]
    [Trait("Category", "Unit")]
    public void Ca_HasBasicConstraintsCaTrue()
    {
        var bc = _certs.Ca.Extensions.OfType<X509BasicConstraintsExtension>().SingleOrDefault();

        bc.Should().NotBeNull("CA certificate must have BasicConstraints extension");
        bc!.CertificateAuthority.Should().BeTrue("CA certificate must have CA:true");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Ca_IsSelfSigned()
    {
        _certs.Ca.Issuer.Should().Be(_certs.Ca.Subject,
            "CA certificate should be self-signed (issuer == subject)");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Ca_HasPrivateKey()
    {
        _certs.Ca.HasPrivateKey.Should().BeTrue(
            "CA certificate must have a private key for signing child certs");
    }

    // ── Broker (server) certificate checks ──

    [Fact]
    [Trait("Category", "Unit")]
    public void Broker_IsNotCa()
    {
        var bc = _certs.Broker.Extensions.OfType<X509BasicConstraintsExtension>().SingleOrDefault();

        bc.Should().NotBeNull();
        bc!.CertificateAuthority.Should().BeFalse("broker cert must NOT be a CA");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Broker_SanContainsLocalhostAndLoopback()
    {
        var san = _certs.Broker.Extensions
            .OfType<X509SubjectAlternativeNameExtension>()
            .SingleOrDefault();

        san.Should().NotBeNull("broker cert must have a Subject Alternative Name extension");

        // .NET 8: parse the formatted SAN string (Format(true) gives multi-line output)
        var sanText = san!.Format(multiLine: true);

        sanText.Should().Contain("localhost",
            "broker SAN must include 'localhost' for local dev/test connections");
        sanText.Should().Contain("127.0.0.1",
            "broker SAN must include '127.0.0.1' for integration tests");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Broker_HasServerAuthEku()
    {
        var eku = _certs.Broker.Extensions
            .OfType<X509EnhancedKeyUsageExtension>()
            .SingleOrDefault();

        eku.Should().NotBeNull("broker cert must have EKU extension");

        var oids = eku!.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value).ToList();
        oids.Should().Contain("1.3.6.1.5.5.7.3.1",
            "broker cert EKU must include serverAuth (OID 1.3.6.1.5.5.7.3.1)");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Broker_IsSignedByCa()
    {
        _certs.Broker.Issuer.Should().Be(_certs.Ca.Subject,
            "broker cert must be signed by the CA");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Broker_ChainValidatesAgainstCa()
    {
        VerifyChainAgainstCa(_certs.Broker, _certs.Ca, shouldSucceed: true);
    }

    // ── Client certificate checks ──

    [Fact]
    [Trait("Category", "Unit")]
    public void Client_HasClientAuthEku()
    {
        foreach (var client in _certs.Clients)
        {
            var eku = client.Extensions
                .OfType<X509EnhancedKeyUsageExtension>()
                .SingleOrDefault();

            eku.Should().NotBeNull(
                $"client cert '{client.Subject}' must have EKU extension");

            var oids = eku!.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value).ToList();
            oids.Should().Contain("1.3.6.1.5.5.7.3.2",
                $"client cert '{client.Subject}' EKU must include clientAuth (OID 1.3.6.1.5.5.7.3.2)");
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Client_CnMatchesRequestedName()
    {
        _certs.Clients.Should().HaveCount(ClientNames.Length);

        for (int i = 0; i < ClientNames.Length; i++)
        {
            var cn = _certs.Clients[i].GetNameInfo(X509NameType.SimpleName, forIssuer: false);
            cn.Should().Be(ClientNames[i],
                $"client cert CN must match the requested client name '{ClientNames[i]}'");
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Client_IsSignedByCa()
    {
        foreach (var client in _certs.Clients)
        {
            client.Issuer.Should().Be(_certs.Ca.Subject,
                $"client cert '{client.Subject}' must be signed by the CA");
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Client_ChainValidatesAgainstCa()
    {
        foreach (var client in _certs.Clients)
        {
            VerifyChainAgainstCa(client, _certs.Ca, shouldSucceed: true);
        }
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Client_IsNotCa()
    {
        foreach (var client in _certs.Clients)
        {
            var bc = client.Extensions.OfType<X509BasicConstraintsExtension>().SingleOrDefault();
            bc.Should().NotBeNull();
            bc!.CertificateAuthority.Should().BeFalse(
                $"client cert '{client.Subject}' must NOT be a CA");
        }
    }

    // ── Untrusted client certificate checks ──

    [Fact]
    [Trait("Category", "Unit")]
    public void Untrusted_DoesNotChainToOurCa()
    {
        VerifyChainAgainstCa(_certs.Untrusted, _certs.Ca, shouldSucceed: false);
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Untrusted_HasClientAuthEku()
    {
        // The untrusted cert should still be a valid-looking client cert,
        // just from a different CA — so it has clientAuth EKU.
        var eku = _certs.Untrusted.Extensions
            .OfType<X509EnhancedKeyUsageExtension>()
            .SingleOrDefault();

        eku.Should().NotBeNull("untrusted cert must have EKU extension");

        var oids = eku!.EnhancedKeyUsages.Cast<Oid>().Select(o => o.Value).ToList();
        oids.Should().Contain("1.3.6.1.5.5.7.3.2",
            "untrusted cert EKU must include clientAuth (it's a valid client cert, just wrong CA)");
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void Untrusted_IssuerDiffersFromCa()
    {
        _certs.Untrusted.Issuer.Should().NotBe(_certs.Ca.Subject,
            "untrusted cert must NOT be issued by our CA");
    }

    // ── File output checks ──

    [Fact]
    [Trait("Category", "Unit")]
    public void GenerateAll_ProducesPfxFiles()
    {
        File.Exists(Path.Combine(_outputDir, "ca.pfx")).Should().BeTrue();
        File.Exists(Path.Combine(_outputDir, "ca.crt")).Should().BeTrue();
        File.Exists(Path.Combine(_outputDir, "broker.pfx")).Should().BeTrue();
        File.Exists(Path.Combine(_outputDir, "client-test-publisher.pfx")).Should().BeTrue();
        File.Exists(Path.Combine(_outputDir, "client-test-subscriber.pfx")).Should().BeTrue();
        File.Exists(Path.Combine(_outputDir, "client-untrusted.pfx")).Should().BeTrue();
    }

    [Fact]
    [Trait("Category", "Unit")]
    public void GenerateAll_PfxFilesAreLoadable()
    {
        // Verify that the exported PFX files can be loaded back as valid certificates
        using var ca = new X509Certificate2(Path.Combine(_outputDir, "ca.pfx"));
        ca.HasPrivateKey.Should().BeTrue();

        using var broker = new X509Certificate2(Path.Combine(_outputDir, "broker.pfx"));
        broker.HasPrivateKey.Should().BeTrue();

        using var client = new X509Certificate2(
            Path.Combine(_outputDir, "client-test-publisher.pfx"));
        client.HasPrivateKey.Should().BeTrue();
    }

    // ── Helper ──

    private static void VerifyChainAgainstCa(
        X509Certificate2 cert,
        X509Certificate2 ca,
        bool shouldSucceed)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.Add(ca);

        var isValid = chain.Build(cert);

        if (shouldSucceed)
        {
            isValid.Should().BeTrue(
                $"cert '{cert.Subject}' should validate against CA '{ca.Subject}'. " +
                $"Chain status: {FormatChainStatus(chain)}");
        }
        else
        {
            isValid.Should().BeFalse(
                $"cert '{cert.Subject}' should NOT validate against CA '{ca.Subject}'");
        }
    }

    private static string FormatChainStatus(X509Chain chain)
    {
        var statuses = chain.ChainStatus
            .Select(s => $"{s.Status}: {s.StatusInformation}")
            .ToArray();
        return statuses.Length == 0 ? "(none)" : string.Join("; ", statuses);
    }
}
