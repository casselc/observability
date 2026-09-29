// Accepted: every policy here compiles to query-service tuples and IAM.

// SRE: the whole of prd, rows and raw objects
@id("sre-prd")
permit (
  principal in Oscope::Group::"sre",
  action in [Oscope::Action::"query", Oscope::Action::"plan"],
  resource in Oscope::Env::"prd"
);

// the search team: rows of its namespace in prd, and its whole dev cluster
@id("search-prd-rows")
permit (
  principal in Oscope::Group::"team-search",
  action == Oscope::Action::"query",
  resource in Oscope::Namespace::"prod-eu-1/search"
);
@id("search-dev")
permit (
  principal in Oscope::Group::"team-search",
  action in [Oscope::Action::"query", Oscope::Action::"plan"],
  resource in Oscope::Cluster::"dev-eu-1"
);

// the payments team's developer tools (D37): its devtools namespace, with
// LLM content (D36)
@id("payments-devtools")
permit (
  principal in Oscope::Group::"approle:Team.Payments",
  action in [Oscope::Action::"query", Oscope::Action::"llm_content"],
  resource == Oscope::Namespace::"devtools/dev-payments"
);

// edges write their own cluster's prefix, in their environment's bucket
@id("edges-prd")
permit (
  principal is Oscope::Workload in Oscope::Group::"edges",
  action == Oscope::Action::"write",
  resource in Oscope::Env::"prd"
) when { resource in principal.cluster };
@id("edges-dev")
permit (
  principal is Oscope::Workload in Oscope::Group::"edges",
  action == Oscope::Action::"write",
  resource in Oscope::Env::"dev"
) when { resource in principal.cluster };

// the Entra ingress (D37) is the devtools cluster's only writer
@id("ingress-devtools")
permit (
  principal is Oscope::Workload in Oscope::Group::"entra-ingress",
  action == Oscope::Action::"write",
  resource in Oscope::Cluster::"devtools"
);
