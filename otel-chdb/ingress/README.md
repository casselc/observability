# ingress: Entra-authenticated OTLP/HTTP ingress for producers outside Kubernetes

DECISIONS.md D37 (proposed); design and STPA in
[../research/entra-ingress.md](../research/entra-ingress.md); validation on a real tenant and
devices in [../deploy/validation/entra-ingress.md](../deploy/validation/entra-ingress.md).

Developer tools (the Langfuse opencode, Codex and Claude Code integrations) and, later, CI jobs
send OTLP/HTTP here through a device forwarder that holds an Entra access token. For each request
the ingress:

1. verifies the bearer token (Entra v2.0, RS256, allowed tenant GUIDs, `iss = {authority}/{tid}/v2.0`,
   tenant-bound keys, audience, client app, scope or app role) — `entra.go`;
2. maps the identity to a tenant namespace by operator policy only (app role or group object id →
   `dev-<team>` under cluster `devtools`) — `mapping.go`;
3. applies per-principal caps (request and decoded-byte buckets, body, decoded and item caps) —
   `limits.go`;
4. stamps the tenant and the person over whatever the producer claimed, deterministically, so a
   retry is a content-key copy (DECISIONS D11) — `stamp.go`;
5. commits through the Go edge library (`../parquetgo/edge`) with its own lanes; 200 only on the
   commit verdict, 503 + `Retry-After` when unresolved — `server.go`;
6. keeps idle lanes alive and, on SIGTERM, drains and writes the D35 close — `lifecycle.go`.

Paths: `POST /v1/traces`, `/v1/logs`, and Langfuse's aliases `/api/public/otel/v1/{traces,logs}`.
`X-Oscope-Namespace` chooses among a principal's grants; it never grants.

## Run

```bash
go run ./cmd/oscope-ingress -config config.json
```

`config.json` (unknown fields are an error):

```json
{
  "listen": ":4318", "producer_id": "ingress-0",
  "s3_url": "s3://bucket/root", "s3_region": "eu-west-1",
  "heartbeat_s": 30, "drain_s": 25,
  "entra": {
    "authority": "https://login.microsoftonline.com",
    "tenants": ["<home tenant id>"],
    "audiences": ["<ingress API client id>", "api://oscope-ingress"],
    "client_apps": ["<forwarder client id>", "<CI app client id>"],
    "user_scope": "Telemetry.Write", "app_role": "Telemetry.Write.App"
  },
  "policy": {
    "cluster": "devtools", "namespace_prefix": "dev-",
    "rules": [
      {"tenant": "<tid>", "role": "Team.Payments", "namespace": "dev-payments"},
      {"tenant": "<tid>", "group": "<group object id>", "namespace": "dev-ops"}
    ]
  }
}
```

S3 credentials come from the AWS SDK's default chain (IRSA / pod identity), never the config.
`producer_id` must be unique per replica (the pod name).

## Test

```bash
go test ./...        # fake Entra issuer (entratest/), in-memory store; no network
```

Everything is tested against the fake issuer only; nothing here has run against a real Entra
tenant or device (the runbook above). Not built: exported metrics (the outcome counters are
in-process, `Server.Counts`), the query service's pair grants. The device forwarder is
[`../forwarder`](../forwarder/README.md) (a streaming pass-through, D40); its end-to-end and
stress tests run against `cmd/ingress-e2e`, a TEST harness: this handler behind a fake Entra, a
broker model, an in-memory store, fault injection, and a numbered answers log (status,
`Retry-After`, body hashes, request header names) for checking a pass-through answer by answer.
