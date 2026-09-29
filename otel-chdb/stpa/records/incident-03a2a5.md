---
id: incident-03a2a5
label: CAST-29
batch: hyperdx-adapter-2026-09-28
title: "`@clickhouse/client` 1.23 replaces a per-query `Authorization` header with its own Basic header, so HyperDX's server-side queries (alerts, API, MCP) would reach the adapter with no token"
found_by: The adapter's integration test driving the real client at HyperDX's pinned version
hazard: "H-8 (availability: server-side queries refused; fail-closed, so no data exposure)"
controller: "Fork patch: \"a per-query header reaches the server as sent\""
why: The client's type definitions accept a per-query `http_headers` map with `Authorization`; nothing said it would be overwritten
fix: "Token sent as the query's `auth: {access_token}`, with a jest test; found before the patch was committed"
lesson: Check a client's wire behaviour with the real client, not its type definitions
state: accepted
---
