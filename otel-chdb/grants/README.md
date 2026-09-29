# grants: Cedar grants compiled to tuples and IAM (prototype, D38)

Grants are written in [Cedar](https://www.cedarpolicy.com/) against
[`schema.cedarschema`](schema.cedarschema) and compiled by `grantc` into what
the pipeline enforces:

- per environment, the query service's `claims.group_grants` as explicit
  `(role, cluster, namespace)` tuples (`queryd-grants-{env}.json`, read by
  `query/internal/auth`, whose scope is the union of tuples, never a
  product; CAST 52);
- IAM for the plan (presign) and write paths, using only S3 prefixes and
  principal tags (D18): `presign-{env}.json` + `-trust.json`,
  `edge-{env}.json`, `write-{group}-{env}.json`.

A policy outside that fragment is **rejected at authoring time with a
reason**, and nothing is written. The design, the fragment and the STPA are
in [research/grants.md](../research/grants.md).

```
go run ./cmd/grantc -schema schema.cedarschema -registry examples/registry.json \
    -out examples/out examples/policies.cedar       # compile
go run ./cmd/grantc -schema schema.cedarschema -registry examples/registry.json \
    examples/rejected.cedar                          # every policy refused, with reasons
ci/iam-lint.sh otel-chdb/grants/examples/out/iam     # from the repository root
```

## What is accepted

| form | compiles to |
|---|---|
| `permit (principal in Group::"g", action in [query, llm_content], resource in Env/Cluster::"…" \| resource in/== Namespace::"c/ns")` | tuples `(role, c, ns)`; an environment is its registered clusters |
| `permit (principal in Group::"g", action == plan, resource in Env/Cluster::"…")` | tuples `(plan, c, *)`; the presign trust policy admits sessions tagged with those clusters |
| `permit (principal is Workload in Group::"g", action == write, resource in Env::"e") when { resource in principal.cluster }` | `edge-{e}.json`: `ROOT/${aws:PrincipalTag/cluster}/*` in e's bucket, and a Deny for a cluster tag not registered in e |
| `permit (principal is Workload in Group::"g", action == write, resource in Cluster::"c")` | `write-{g}-{e}.json` with c's literal prefix |

## What is refused (reason token: why)

`plan_namespace`, `write_namespace`: namespace is not a prefix tier (raw
objects hold every namespace of their cluster); a namespace grant may give
query, never plan. `condition_not_expressible`: any other `when`/`unless`.
`forbid_not_compiled`: grants are a union of permits. `principal_person`,
`principal_unconstrained`, `principal_not_group`,
`principal_type_not_compiled` (`is Workload` on a read: the query service
cannot tell a workload's group token from a person's). `action_unconstrained`,
`action_unknown`, `admin_not_compiled`. `resource_unconstrained` (every
environment), `resource_eq_container`, `resource_type_only`,
`resource_unregistered`, `resource_bad_namespace`. `write_person`,
`write_env_literal`, `tag_write_needs_env`. `schema`: cedar-go's
(experimental, `x/exp/schema/validate`) strict validator.

## How it is checked

- `Check` compares the output with **Cedar's own authorizer** (cedar-go
  v1.8.0) on every request of the registry's finite universe (every
  environment, every registered cluster and one unregistered, every known
  namespace and one unnamed, a person and a workload per group and one
  holding every group; query, plan, llm_content, and write by a workload of
  each cluster); `grantc` refuses the set on any disagreement.
- `TestGeneratedPoliciesAgreeWithCedar` (rapid): random policy sets over
  the grammar, accepted and refused forms; the property found a compiler bug
  on its first run (`is Workload` on a read compiled to a group-wide
  tuple), now refused as `principal_type_not_compiled`.
- `TestCheckCatchesCompilerMutants`: seven output mutants (a namespace
  widened, a tuple dropped, moved to another environment, plan added, a role
  crossed, a tag write moved, a literal write widened) are refused.
- `TestCompiledIAMMatchesTheCheck`: a small model of IAM evaluation (not
  AWS) evaluates the committed documents over every cluster pair.
- `TestExamplesCompileAgreeAndAreCommitted`: `examples/out` is what the
  examples compile to; CI lints it with `ci/iam-lint.sh`.
- `query/internal/auth` `TestCompiledCedarGrantsLoad`: the query service
  reads the compiled `queryd-grants-*.json`.

## Limits

Prototype: not wired into deployment; the IAM was **not run on AWS** [D];
the presign-session path in the query service (assume a role per plan
cluster with session tags) is designed, not built — the planner still
presigns with its own credentials, cut by the tuples. cedar-go has no
policy templates, partial evaluation (only `x/exp/batch`) or formatter;
its schema validator is experimental. The registry is a file here; see
research/grants.md §5 for where it should come from.
