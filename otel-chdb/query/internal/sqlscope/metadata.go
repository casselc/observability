package sqlscope

import (
	"fmt"
	"reflect"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// MetadataColumns are the columns a metadata table serves when its
// configuration names none: what HyperDX 2.39.1 reads (common-utils
// core/metadata.ts, TableMetadata's used fields; README §8) and what the
// adapter's DESCRIBE / SHOW rewrites select. Everything else is withheld:
// system.tables' data_paths and metadata_path are server file paths, uuid
// names the on-disk store directory, storage_policy and the byte and part
// counts describe the server, and system.databases' engine_full can hold
// a remote database's connection. create_table_query, engine_full and
// as_select are served (HyperDX finds rollups and Distributed targets
// through them); ClickHouse itself masks the secrets in them ('[HIDDEN]',
// probed on 26.10 for S3 keys and URL passwords; DECISIONS.md D26).
var MetadataColumns = map[string][]string{
	// table is system.tables' alias of name (HyperDX's onboarding check
	// filters on it)
	"tables": {"database", "name", "table", "engine", "is_temporary", "create_table_query", "engine_full", "as_select",
		"partition_key", "sorting_key", "primary_key", "sampling_key", "total_rows", "comment"},
	"columns": {"database", "table", "name", "type", "position", "default_kind", "default_expression", "comment",
		"compression_codec", "is_in_partition_key", "is_in_sorting_key", "is_in_primary_key", "is_in_sampling_key"},
	"data_skipping_indices": {"database", "table", "name", "type", "type_full", "expr", "granularity"},
	"settings":              {"name", "value", "changed"},
	"table_engines":         {"name"},
	"databases":             {"name", "engine", "comment"},
}

// MetadataRestricted are the metadata columns served differently to a
// caller without every cluster and namespace (a table's
// restricted_columns overrides it): column -> the expression served under
// its name. system.tables' total_rows counts every cluster's rows; a
// cluster-restricted caller gets 1 for a table with rows, 0 for an empty
// one (NULL stays NULL). HyperDX reads total_rows in one place only, the
// onboarding checklist's "is there data" (useOnboardingCompletion.ts:
// sum(total_rows) > 0 over the sources' tables), which this keeps working
// without disclosing the fleet's volumes (query README §3).
var MetadataRestricted = map[string]map[string]string{
	"tables": {"total_rows": "toUInt64(total_rows > 0)"},
}

// metadataProjection fixes t's columns and the projection every read of it
// is replaced with.
func (t *Table) metadataProjection() error {
	if len(t.Columns) == 0 {
		t.Columns = MetadataColumns[t.Name]
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %s: a metadata table needs a columns allow-list (no default for %s)", t.FQN(), t.Name)
	}
	seen := map[string]bool{}
	for _, c := range t.Columns {
		if !nameRE.MatchString(c) || seen[c] {
			return fmt.Errorf("table %s: column %q is not a plain name, or is listed twice", t.FQN(), c)
		}
		seen[c] = true
	}
	if t.RestrictedColumns == nil {
		t.RestrictedColumns = MetadataRestricted[t.Name]
	}
	restricted := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		restricted[i] = c
		if x, ok := t.RestrictedColumns[c]; ok {
			e, err := parseExpr(x)
			if err != nil {
				return fmt.Errorf("table %s: restricted column %s: %w", t.FQN(), c, err)
			}
			restricted[i] = chp.Format(e) + " AS " + c
		}
	}
	for c := range t.RestrictedColumns {
		if !seen[c] {
			return fmt.Errorf("table %s: restricted column %q is not one of its columns", t.FQN(), c)
		}
	}
	for i, cols := range [][]string{t.Columns, restricted} {
		sql := "SELECT " + strings.Join(cols, ", ") + " FROM " + t.Database + "." + t.Name
		st, err := chp.NewParser(sql).ParseStmts()
		if err != nil || len(st) != 1 || chp.Format(st[0]) != sql {
			return fmt.Errorf("table %s: the projection %q does not parse to itself: %v", t.FQN(), sql, err)
		}
		if i == 0 {
			t.projection = sql
		} else {
			t.restrictedProjection = sql
		}
	}
	return nil
}

// metaTable is the metadata table ti names, or nil.
func (p *Policy) metaTable(ti *chp.TableIdentifier) *Table {
	if ti == nil || ti.Table == nil || ti.Database == nil {
		return nil // Prepare qualifies every served table; a bare name is a CTE
	}
	t := p.Tables[ti.Database.Name+"."+ti.Table.Name]
	if t == nil || t.Scope != "metadata" {
		return nil
	}
	return t
}

// isProjection: sq is exactly some metadata table's projection (for a
// restricted caller, its restricted projection: a caller who writes the
// full projection out gets it projected again, restricted).
func (p *Policy) isProjection(sq *chp.SelectQuery, restricted bool) bool {
	if sq.From == nil {
		return false
	}
	var ti *chp.TableIdentifier
	if j, ok := sq.From.Expr.(*chp.JoinTableExpr); ok && j.Table != nil {
		ti, _ = j.Table.Expr.(*chp.TableIdentifier)
	}
	t := p.metaTable(ti)
	return t != nil && chp.Format(sq) == t.projectionFor(restricted)
}

func (t *Table) projectionFor(restricted bool) string {
	if restricted {
		return t.restrictedProjection
	}
	return t.projection
}

// projectMetadata replaces each read of a metadata table, `system.tables`
// or `system.tables AS t`, with `(SELECT <columns> FROM system.tables) AS
// tables` (or AS t). A `*` then expands to the allow-listed columns only,
// and a name outside them is ClickHouse's UNKNOWN_IDENTIFIER: the columns
// are cut where the rows are read, whatever the statement does with them.
func (pr *Prepared) projectMetadata(restricted bool) {
	p := pr.policy
	visit(reflect.ValueOf(pr.root), func(n chp.Expr) bool {
		switch x := n.(type) {
		case *chp.SelectQuery:
			return !p.isProjection(x, restricted) // already projected: idempotent
		case *chp.TableExpr:
			switch e := x.Expr.(type) {
			case *chp.TableIdentifier:
				if t := p.metaTable(e); t != nil {
					x.Expr = &chp.AliasExpr{Expr: projectionOf(t, restricted), Alias: &chp.Ident{Name: t.Name, QuoteType: chp.BackTicks}}
					return false
				}
			case *chp.AliasExpr:
				if ti, ok := e.Expr.(*chp.TableIdentifier); ok {
					if t := p.metaTable(ti); t != nil {
						e.Expr = projectionOf(t, restricted)
						return false
					}
				}
			}
		}
		return true
	})
}

func projectionOf(t *Table, restricted bool) chp.Expr {
	st, _ := chp.NewParser(t.projectionFor(restricted)).ParseStmts() // checked in NewPolicy
	return &chp.SubQuery{HasParen: true, Select: st[0].(*chp.SelectQuery)}
}

// checkProjected refuses a statement that reads a metadata table other than
// through its projection (fail closed: a place the rewrite did not reach).
func (p *Policy) checkProjected(root chp.Expr, restricted bool) error {
	var bad string
	visit(reflect.ValueOf(root), func(n chp.Expr) bool {
		if bad != "" {
			return false
		}
		switch x := n.(type) {
		case *chp.SelectQuery:
			return !p.isProjection(x, restricted)
		case *chp.TableIdentifier:
			if t := p.metaTable(x); t != nil {
				bad = t.FQN()
			}
		}
		return true
	})
	if bad != "" {
		return reject("metadata_unprojected", "table %s is read other than through its allow-listed columns", bad)
	}
	return nil
}
