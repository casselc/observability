using System.Net.Http.Json;
using System.Text.Json;
using Oscope.Forwarder;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Http;

// The forwarder for the end-to-end and stress jobs (ci/forwarder.sh): its token source is
// the Go harness's broker model; everything else is the device forwarder's own code.
//   OSCOPE_E2E_INGRESS   the harness, e.g. http://127.0.0.1:18431
//   OSCOPE_E2E_PORT      the local port (default 14318)
//   OSCOPE_E2E_PROXY     ProxyOptions as JSON (optional)
//   OSCOPE_E2E_PK / _SK  the local key pair
string Env(string name, string? dflt = null) =>
    Environment.GetEnvironmentVariable(name) ?? dflt ?? throw new InvalidOperationException($"{name} is required");
var web = new JsonSerializerOptions(JsonSerializerDefaults.Web) { UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow };
var ingress = new Uri(Env("OSCOPE_E2E_INGRESS"));
var proxyJson = Environment.GetEnvironmentVariable("OSCOPE_E2E_PROXY");
var settings = new ForwarderSettings
{
    Ingress = ingress,
    Local = new LocalOptions
    {
        Port = int.Parse(Env("OSCOPE_E2E_PORT", "14318"), System.Globalization.CultureInfo.InvariantCulture),
        PublicKey = Env("OSCOPE_E2E_PK", "pk-lf-local-e2e"),
        SecretKey = Env("OSCOPE_E2E_SK", "sk-lf-local-e2e-0123456789"),
    },
    Proxy = string.IsNullOrEmpty(proxyJson) ? new ProxyOptions() : JsonSerializer.Deserialize<ProxyOptions>(proxyJson, web)!,
}.Validate();
var fwd = ForwarderHost.Build(settings, new HarnessBroker(ingress, $"device-{settings.Local.Port}"), args);
await fwd.App.StartAsync();
Console.WriteLine($"forwarder listening http://127.0.0.1:{settings.Local.Port}");
await fwd.App.WaitForShutdownAsync();
// Nothing is held, so there is nothing to account for at exit but what was proxied.
var l = fwd.Local;
Console.WriteLine("forwarder stopped: proxied=" + string.Join(",", l.Proxied.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}:{kv.Value}"))
    + " errors=" + string.Join(",", l.ProxyErrors.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}:{kv.Value}"))
    + " refused=" + string.Join(",", l.Refused.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}:{kv.Value}"))
    + $" in_flight={l.InFlight}");
return 0;

/// <summary>The harness's broker model over HTTP (ingress/cmd/ingress-e2e: /_e2e/broker/token).</summary>
internal sealed class HarnessBroker(Uri ingress, string device) : ITokenAcquirer
{
    private readonly HttpClient _http = new() { BaseAddress = ingress, Timeout = TimeSpan.FromSeconds(10) };
    private readonly Lock _gate = new();
    private AccountKey? _current;

    public AccountKey? CurrentAccount
    {
        get
        {
            lock (_gate) return _current;
        }
    }

    public async ValueTask<TokenResult> AcquireSilentAsync(bool forceRefresh, CancellationToken ct)
    {
        var url = $"/_e2e/broker/token?client={device}&force={(forceRefresh ? 1 : 0)}";
        JsonElement doc;
        try
        {
            doc = await _http.GetFromJsonAsync<JsonElement>(url, ct).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            return TokenResult.Failed(TokenStatus.Unavailable, ex.Message);
        }
        var status = doc.GetProperty("status").GetString();
        switch (status)
        {
            case "ok":
                var acct = new AccountKey(doc.GetProperty("tenant_id").GetString()!, doc.GetProperty("object_id").GetString()!);
                lock (_gate) _current = acct;
                return new TokenResult(TokenStatus.Ok, doc.GetProperty("access_token").GetString(), acct,
                    DateTimeOffset.FromUnixTimeSeconds(doc.GetProperty("expires_on").GetInt64()));
            case "interaction_required":
                lock (_gate) _current = null;
                return TokenResult.Failed(TokenStatus.InteractionRequired, status);
            default:
                return TokenResult.Failed(TokenStatus.Unavailable, status ?? "?");
        }
    }
}
