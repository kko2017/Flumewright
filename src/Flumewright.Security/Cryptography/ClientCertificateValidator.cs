using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Flumewright.Security.Cryptography;

public static class ClientCertificateValidator
{
    private const string ClientAuthEkuOid = "1.3.6.1.5.5.7.3.2";

    public static bool Validate(X509Certificate2? clientCert, X509Certificate2 caCert)
    {
        if (clientCert == null)
            return false;

        // Ensure the certificate is intended for Client Authentication
        var hasClientAuth = clientCert.Extensions
            .OfType<X509EnhancedKeyUsageExtension>()
            .Any(eku => eku.EnhancedKeyUsages.Cast<System.Security.Cryptography.Oid>().Any(oid => oid.Value == ClientAuthEkuOid));

        if (!hasClientAuth)
            return false;

        using var customChain = new X509Chain();
        // local CA publishes no CRL/OCSP; revocation has nothing to check against; revisit if a real CA is ever used.
        customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        customChain.ChainPolicy.CustomTrustStore.Add(caCert);

        return customChain.Build(clientCert);
    }
}
