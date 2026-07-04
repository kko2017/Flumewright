using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Flumewright.CertGen;

/// <summary>
/// Generates a certificate chain for Flumewright mTLS:
/// a self-signed CA, a broker (server) cert, client certs, and an untrusted client cert.
/// </summary>
public static class CertificateGenerator
{
    private const int KeySizeBits = 2048;
    private static readonly TimeSpan CertValidity = TimeSpan.FromDays(365);
    private static readonly HashAlgorithmName SignatureAlgorithm = HashAlgorithmName.SHA256;

    /// <summary>
    /// Generates the full certificate chain and writes PFX files to <paramref name="outputDir"/>.
    /// </summary>
    /// <param name="outputDir">Directory to write certificate files into.</param>
    /// <param name="clientNames">Client names (each becomes the CN of a client cert).</param>
    /// <param name="password">PFX password (null for no password).</param>
    public static GeneratedCertificates GenerateAll(
        string outputDir,
        IReadOnlyList<string> clientNames,
        string? password = null)
    {
        ArgumentNullException.ThrowIfNull(outputDir);
        ArgumentNullException.ThrowIfNull(clientNames);

        Directory.CreateDirectory(outputDir);

        // 1. Self-signed CA
        var ca = CreateCaCertificate();

        // 2. Broker (server) cert signed by CA
        var broker = CreateBrokerCertificate(ca);

        // 3. Client certs signed by CA
        var clients = new List<X509Certificate2>();
        foreach (var name in clientNames)
        {
            clients.Add(CreateClientCertificate(ca, name));
        }

        // 4. Untrusted client cert (self-signed by a throwaway CA — not chained to our CA)
        var untrustedCa = CreateCaCertificate("CN=Flumewright Untrusted CA");
        var untrusted = CreateClientCertificate(untrustedCa, "untrusted-client");
        untrustedCa.Dispose();

        // Export to PFX files
        var result = new GeneratedCertificates(ca, broker, clients, untrusted);
        ExportAll(result, outputDir, password);

        return result;
    }

    /// <summary>
    /// Creates a self-signed CA certificate with BasicConstraints CA:true.
    /// </summary>
    public static X509Certificate2 CreateCaCertificate(
        string subjectName = "CN=Flumewright Dev CA")
    {
        using var key = RSA.Create(KeySizeBits);
        var request = new CertificateRequest(
            subjectName,
            key,
            SignatureAlgorithm,
            RSASignaturePadding.Pkcs1);

        // CA: true, critical, no path-length constraint
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(
                certificateAuthority: true,
                hasPathLengthConstraint: false,
                pathLengthConstraint: 0,
                critical: true));

        // Key usage: cert signing + CRL signing
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign,
                critical: true));

        // Subject Key Identifier
        request.CertificateExtensions.Add(
            new X509SubjectKeyIdentifierExtension(request.PublicKey, critical: false));

        var notBefore = DateTimeOffset.UtcNow;
        var notAfter = notBefore.Add(CertValidity);

        var cert = request.CreateSelfSigned(notBefore, notAfter);
        // Re-export to get an X509Certificate2 with a usable private key on all platforms
        return new X509Certificate2(
            cert.Export(X509ContentType.Pfx),
            (string?)null,
            X509KeyStorageFlags.Exportable);
    }

    /// <summary>
    /// Creates a broker (server) certificate signed by the CA.
    /// SAN includes localhost and 127.0.0.1 (required for integration tests).
    /// </summary>
    public static X509Certificate2 CreateBrokerCertificate(X509Certificate2 caCert)
    {
        using var key = RSA.Create(KeySizeBits);
        var request = new CertificateRequest(
            "CN=Flumewright Broker",
            key,
            SignatureAlgorithm,
            RSASignaturePadding.Pkcs1);

        // Not a CA
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(
                certificateAuthority: false,
                hasPathLengthConstraint: false,
                pathLengthConstraint: 0,
                critical: true));

        // Key usage: digital signature + key encipherment (standard for TLS server)
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
                critical: true));

        // EKU: serverAuth
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(
                new OidCollection
                {
                    new("1.3.6.1.5.5.7.3.1") // serverAuth
                },
                critical: false));

        // SAN: localhost + 127.0.0.1
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddDnsName("localhost");
        sanBuilder.AddIpAddress(IPAddress.Parse("127.0.0.1"));
        request.CertificateExtensions.Add(sanBuilder.Build());

        // Subject Key Identifier
        request.CertificateExtensions.Add(
            new X509SubjectKeyIdentifierExtension(request.PublicKey, critical: false));

        var serial = GenerateSerialNumber();
        var notBefore = caCert.NotBefore;
        var notAfter = caCert.NotAfter;

        using var signedCert = request.Create(caCert, notBefore, notAfter, serial);
        // Attach private key and re-export for a usable cert
        var certWithKey = signedCert.CopyWithPrivateKey(key);
        return new X509Certificate2(
            certWithKey.Export(X509ContentType.Pfx),
            (string?)null,
            X509KeyStorageFlags.Exportable);
    }

    /// <summary>
    /// Creates a client certificate signed by the CA.
    /// EKU: clientAuth. CN = <paramref name="clientName"/>.
    /// </summary>
    public static X509Certificate2 CreateClientCertificate(
        X509Certificate2 caCert,
        string clientName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientName);

        using var key = RSA.Create(KeySizeBits);
        var request = new CertificateRequest(
            $"CN={clientName}",
            key,
            SignatureAlgorithm,
            RSASignaturePadding.Pkcs1);

        // Not a CA
        request.CertificateExtensions.Add(
            new X509BasicConstraintsExtension(
                certificateAuthority: false,
                hasPathLengthConstraint: false,
                pathLengthConstraint: 0,
                critical: true));

        // Key usage: digital signature
        request.CertificateExtensions.Add(
            new X509KeyUsageExtension(
                X509KeyUsageFlags.DigitalSignature,
                critical: true));

        // EKU: clientAuth
        request.CertificateExtensions.Add(
            new X509EnhancedKeyUsageExtension(
                new OidCollection
                {
                    new("1.3.6.1.5.5.7.3.2") // clientAuth
                },
                critical: false));

        // Subject Key Identifier
        request.CertificateExtensions.Add(
            new X509SubjectKeyIdentifierExtension(request.PublicKey, critical: false));

        var serial = GenerateSerialNumber();
        var notBefore = caCert.NotBefore;
        var notAfter = caCert.NotAfter;

        using var signedCert = request.Create(caCert, notBefore, notAfter, serial);
        var certWithKey = signedCert.CopyWithPrivateKey(key);
        return new X509Certificate2(
            certWithKey.Export(X509ContentType.Pfx),
            (string?)null,
            X509KeyStorageFlags.Exportable);
    }

    private static void ExportAll(
        GeneratedCertificates certs,
        string outputDir,
        string? password)
    {
        ExportPfx(certs.Ca, Path.Combine(outputDir, "ca.pfx"), password);
        ExportPfx(certs.Broker, Path.Combine(outputDir, "broker.pfx"), password);

        for (int i = 0; i < certs.Clients.Count; i++)
        {
            var client = certs.Clients[i];
            var cn = client.GetNameInfo(X509NameType.SimpleName, forIssuer: false) ?? $"client{i}";
            ExportPfx(client, Path.Combine(outputDir, $"client-{cn}.pfx"), password);
        }

        ExportPfx(certs.Untrusted, Path.Combine(outputDir, "client-untrusted.pfx"), password);

        // Also export CA cert (public key only) for trust-store registration
        var caCertBytes = certs.Ca.Export(X509ContentType.Cert);
        File.WriteAllBytes(Path.Combine(outputDir, "ca.crt"), caCertBytes);
    }

    private static void ExportPfx(X509Certificate2 cert, string path, string? password)
    {
        var pfxBytes = cert.Export(X509ContentType.Pfx, password);
        File.WriteAllBytes(path, pfxBytes);
    }

    private static byte[] GenerateSerialNumber()
    {
        var serial = new byte[16];
        RandomNumberGenerator.Fill(serial);
        // Ensure positive (ASN.1 INTEGER)
        serial[0] &= 0x7F;
        // Ensure non-zero leading byte
        if (serial[0] == 0) serial[0] = 1;
        return serial;
    }
}

/// <summary>
/// Holds the generated certificates. Implements IDisposable to clean up native resources.
/// </summary>
public sealed class GeneratedCertificates : IDisposable
{
    public X509Certificate2 Ca { get; }
    public X509Certificate2 Broker { get; }
    public IReadOnlyList<X509Certificate2> Clients { get; }
    public X509Certificate2 Untrusted { get; }

    public GeneratedCertificates(
        X509Certificate2 ca,
        X509Certificate2 broker,
        IReadOnlyList<X509Certificate2> clients,
        X509Certificate2 untrusted)
    {
        Ca = ca;
        Broker = broker;
        Clients = clients;
        Untrusted = untrusted;
    }

    public void Dispose()
    {
        Ca.Dispose();
        Broker.Dispose();
        foreach (var c in Clients) c.Dispose();
        Untrusted.Dispose();
    }
}
