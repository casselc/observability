// Each policy here is refused by grantc, with the reason in its @expect.

@id("plan-on-a-namespace") @expect("plan_namespace")
permit (principal in Oscope::Group::"team-search", action in [Oscope::Action::"query", Oscope::Action::"plan"], resource in Oscope::Namespace::"prod-eu-1/search");

@id("every-environment") @expect("resource_unconstrained")
permit (principal in Oscope::Group::"sre", action == Oscope::Action::"query", resource);

@id("a-person-by-name") @expect("principal_person")
permit (principal == Oscope::User::"3f2c9c1e-0000-4000-8000-000000000001", action == Oscope::Action::"query", resource in Oscope::Cluster::"prod-eu-1");

@id("everyone") @expect("principal_unconstrained")
permit (principal, action == Oscope::Action::"query", resource in Oscope::Env::"dev");

@id("a-forbid") @expect("forbid_not_compiled")
forbid (principal in Oscope::Group::"contractors", action == Oscope::Action::"query", resource in Oscope::Env::"prd");

@id("a-condition-on-context") @expect("condition_not_expressible")
permit (principal in Oscope::Group::"sre", action == Oscope::Action::"query", resource in Oscope::Env::"prd") when { context has break_glass };

@id("every-action") @expect("action_unconstrained")
permit (principal in Oscope::Group::"sre", action, resource in Oscope::Env::"dev");

@id("env-as-entity-only") @expect("resource_eq_container")
permit (principal in Oscope::Group::"sre", action == Oscope::Action::"query", resource == Oscope::Env::"prd");

@id("write-a-namespace") @expect("write_namespace")
permit (principal is Oscope::Workload in Oscope::Group::"edges", action == Oscope::Action::"write", resource in Oscope::Namespace::"prod-eu-1/shop");

@id("write-a-whole-env-untagged") @expect("write_env_literal")
permit (principal is Oscope::Workload in Oscope::Group::"edges", action == Oscope::Action::"write", resource in Oscope::Env::"prd");

@id("unregistered-cluster") @expect("resource_unregistered")
permit (principal in Oscope::Group::"sre", action == Oscope::Action::"query", resource in Oscope::Cluster::"prod-ap-9");

@id("admin") @expect("admin_not_compiled")
permit (principal in Oscope::Group::"sre", action == Oscope::Action::"admin", resource in Oscope::Env::"prd");
