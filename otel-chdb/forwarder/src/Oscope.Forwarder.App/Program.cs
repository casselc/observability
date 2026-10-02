using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using Oscope.Forwarder;
using Oscope.Forwarder.App;
using Oscope.Forwarder.Http;

// oscope-forwarder: the device forwarder (DECISIONS.md D37, D40 as amended 2026-10-02: a streaming
// pass-through; research/entra-ingress.md §4, §10b).
//   oscope-forwarder [--config <MDM config.json>] [--keys <per-user keys.json>]
//   oscope-forwarder --init-keys [--keys <path>]   writes this user's random local key pair
//                                                  and prints the tools' environment
var json = new JsonSerializerOptions(JsonSerializerDefaults.Web)
{
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, // a misspelt bound is an error, not a default
};
string? Arg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}
var dir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "oscope-forwarder");
var keysPath = Arg("--keys") ?? Path.Combine(dir, "keys.json");
var configPath = Arg("--config") ?? Environment.GetEnvironmentVariable("OSCOPE_FORWARDER_CONFIG") ?? Path.Combine(dir, "config.json");

if (args.Contains("--init-keys"))
{
    // The local key pair is a secret between this user's tools and this user's forwarder
    // (LS-E2); it names nothing outside the device. Not telemetry: the forwarder keeps no
    // telemetry on disk (R-E7).
    var keys = new LocalKeys("pk-lf-local-" + Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(8)),
        "sk-lf-local-" + Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(24)));
    Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(keysPath))!);
    var opts = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
    if (!OperatingSystem.IsWindows()) opts.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
    using (var f = new FileStream(keysPath, opts)) JsonSerializer.Serialize(f, keys, json);
    var port = File.Exists(configPath) ? Load<AppConfig>(configPath).Port : 14318;
    Console.WriteLine($"LANGFUSE_BASE_URL=http://127.0.0.1:{port}");
    Console.WriteLine($"LANGFUSE_PUBLIC_KEY={keys.PublicKey}");
    Console.WriteLine($"LANGFUSE_SECRET_KEY={keys.SecretKey}");
    Console.WriteLine("LANGFUSE_MEDIA_UPLOAD_ENABLED=false");
    return 0;
}

var cfg = Load<AppConfig>(configPath);
var k = Load<LocalKeys>(keysPath);
var settings = new ForwarderSettings
{
    Ingress = cfg.Ingress,
    Namespace = cfg.Namespace,
    Local = new LocalOptions { Port = cfg.Port, PublicKey = k.PublicKey, SecretKey = k.SecretKey },
    Proxy = cfg.Proxy ?? new ProxyOptions(),
}.Validate();
var fwd = ForwarderHost.Build(settings, new MsalBrokerTokenAcquirer(cfg.Entra), args);
await fwd.App.RunAsync().ConfigureAwait(false);
return 0;

T Load<T>(string path) =>
    JsonSerializer.Deserialize<T>(File.ReadAllBytes(path), json) ?? throw new InvalidDataException($"{path}: empty");

/// <summary>The MDM-delivered configuration (research/entra-ingress.md §4.5).</summary>
internal sealed record AppConfig(Uri Ingress, EntraOptions Entra, int Port = 14318, string? Namespace = null,
    ProxyOptions? Proxy = null);

/// <summary>This user's local key pair (§4.3).</summary>
internal sealed record LocalKeys(string PublicKey, string SecretKey);
