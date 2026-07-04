using Flumewright.CertGen;

// Default output directory and client names
var outputDir = args.Length > 0 ? args[0] : "./certs";
var clientNames = args.Length > 1
    ? args.Skip(1).ToList()
    : new List<string> { "publisher", "subscriber" };

Console.WriteLine($"Flumewright Certificate Generator");
Console.WriteLine($"Output directory: {Path.GetFullPath(outputDir)}");
Console.WriteLine($"Client names: {string.Join(", ", clientNames)}");
Console.WriteLine();

using var certs = CertificateGenerator.GenerateAll(outputDir, clientNames);

Console.WriteLine("Generated certificates:");
Console.WriteLine($"  CA:         ca.pfx (+ ca.crt public key)");
Console.WriteLine($"  Broker:     broker.pfx");
foreach (var client in certs.Clients)
{
    var cn = client.GetNameInfo(
        System.Security.Cryptography.X509Certificates.X509NameType.SimpleName,
        forIssuer: false);
    Console.WriteLine($"  Client:     client-{cn}.pfx");
}
Console.WriteLine($"  Untrusted:  client-untrusted.pfx");
Console.WriteLine();
Console.WriteLine("Done. These files are .gitignore'd — do NOT commit them.");
