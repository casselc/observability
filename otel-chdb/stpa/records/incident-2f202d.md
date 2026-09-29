---
id: incident-2f202d
label: CAST-52
batch: ci-sweep-after-d35-2026-09-29
title: "The query service's grants are a cluster set × a namespace set, not pairs: a principal granted `devtools/dev-payments` and `prod-eu-1/search` can also read `prod-eu-1/dev-payments` and `devtools/search` (`query/internal/auth/principal.go`)"
found_by: Reading the grant code while designing the devtools tenant key (D37)
hazard: "H-6, H-E3: reads beyond the intended scope once any principal holds grants in two clusters with different namespaces"
controller: "Query service: \"a principal's scope is one cluster set and one namespace set\""
why: Every scope so far was one cluster, or all clusters, so the product and the pairs were equal
fix: "Grants are (role, cluster, namespace) tuples combined as a union (`Principal.For` per role); two more over-reaches found and fixed on the way: roles pooled across grants (a plan grant widened a query grant) and the catalog cache keyed by the two lists rather than the pairs. `TestPairScopeIsUnionNeverProduct`, `TestEffectiveScopeIsUnionOfTuples`, `TestQueryGrantsAreUnionNotProduct`, `TestPlanUsesOnlyWholeClusterPlanGrants`; the old product is a caught mutant (differs from the union in 365 of 1,000 generated scenarios); commit a4c5470 (D38)"
lesson: "CAST theme 36 again: a scope expressed as two independent sets means more than any grant intended; express grants in the shape of the thing granted"
variables: [qs/grants]
state: accepted
---
