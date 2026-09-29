// Command grantc compiles Cedar grant policies (DECISIONS.md D38) into the
// query service's per-environment tuples and IAM policies, refusing any
// policy outside the expressible fragment with its reason, and checking
// the result against Cedar's authorizer.
//
//	grantc -schema schema.cedarschema -registry registry.json -out DIR policies.cedar...
//
// Exit status 1 when any policy is rejected or the check fails; nothing is
// written then.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/casselc/observability/otel-chdb/grants"
	cedar "github.com/cedar-policy/cedar-go"
)

func main() {
	schemaPath := flag.String("schema", "schema.cedarschema", "the Cedar schema")
	regPath := flag.String("registry", "registry.json", "environments and their clusters")
	outDir := flag.String("out", "", "directory for the compiled files (empty: check only)")
	flag.Parse()
	if err := run(*schemaPath, *regPath, *outDir, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "grantc:", err)
		os.Exit(1)
	}
}

func run(schemaPath, regPath, outDir string, files []string) error {
	st, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}
	v, err := grants.LoadSchema(st)
	if err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	reg, err := grants.LoadRegistry(regPath)
	if err != nil {
		return err
	}
	var all cedar.PolicyList
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		ps, err := grants.Parse(f, b)
		if err != nil {
			return err
		}
		all = append(all, ps...)
	}
	out, rej := grants.Compile(v, all, reg)
	for _, r := range rej {
		fmt.Fprintln(os.Stderr, "rejected:", r.Error())
	}
	if len(rej) > 0 {
		return fmt.Errorf("%d of %d policies rejected: %w", len(rej), len(all), grants.ErrRejected)
	}
	if err := grants.Check(all, out, reg); err != nil {
		return err
	}
	fmt.Printf("%d policies compiled; the output agrees with Cedar on every request of the registry's universe\n", len(all))
	if outDir == "" {
		return nil
	}
	docs, err := grants.IAM(out, reg)
	if err != nil {
		return err
	}
	for e := range reg.Envs {
		docs["queryd-grants-"+e+".json"] = grants.QueryConfig(out, e)
	}
	if err := os.MkdirAll(filepath.Join(outDir, "iam"), 0o755); err != nil {
		return err
	}
	names := make([]string, 0, len(docs))
	for n := range docs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		p := filepath.Join(outDir, n)
		if filepath.Ext(n) == ".json" && n[:6] != "queryd" {
			p = filepath.Join(outDir, "iam", n)
		}
		if err := os.WriteFile(p, grants.Marshal(docs[n]), 0o644); err != nil {
			return err
		}
		fmt.Println("wrote", p)
	}
	return nil
}
