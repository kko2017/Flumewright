using Microsoft.Extensions.Configuration;

namespace Flumewright.Broker.Configuration;

public record MtlsConfig(bool RequireClientCertificate, string? ServerCertPath, string? CaCertPath)
{
    public static MtlsConfig FromConfiguration(IConfiguration configuration)
    {
        var requireClientCert = configuration.GetValue<bool>("Broker:RequireClientCertificate", false);
        var serverCertPath = configuration["Broker:ServerCertPath"];
        var caCertPath = configuration["Broker:CaCertPath"];

        if (requireClientCert)
        {
            if (string.IsNullOrWhiteSpace(serverCertPath) || !File.Exists(serverCertPath))
            {
                throw new InvalidOperationException($"mTLS is enabled but ServerCertPath '{serverCertPath}' is missing or invalid.");
            }

            if (string.IsNullOrWhiteSpace(caCertPath) || !File.Exists(caCertPath))
            {
                throw new InvalidOperationException($"mTLS is enabled but CaCertPath '{caCertPath}' is missing or invalid.");
            }
        }

        return new MtlsConfig(requireClientCert, serverCertPath, caCertPath);
    }
}
