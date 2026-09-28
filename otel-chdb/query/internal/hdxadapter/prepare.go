package hdxadapter

import (
	"reflect"
	"regexp"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Statement is what the adapter sends to the service for one HyperDX
// statement, and how it answers.
type Statement struct {
	// Kind: "select", "describe", "show_databases", "show_tables".
	Kind   string
	SQL    string // for POST /v1/query
	Format string // the output format HyperDX asked for
	Window *Window
	// Metadata: the statement reads only system tables (or none): its answer
	// is schema, and carries no completeness label.
	Metadata bool
}

// Formats the adapter renders from the service's FORMAT JSON answer.
var Formats = map[string]bool{
	"JSON": true, "JSONEachRow": true, "JSONCompact": true, "JSONCompactEachRow": true,
	"JSONCompactEachRowWithNames": true, "JSONCompactEachRowWithNamesAndTypes": true,
}

// DescribeSQL is DESCRIBE TABLE as a SELECT on system.columns, with
// DESCRIBE's column names and order (ClickHouse 26.10). ttl_expression is
// not in system.columns and is always empty here.
const DescribeSQL = `SELECT name, type, default_kind AS default_type, default_expression, comment,
  if(startsWith(compression_codec, 'CODEC('), substring(compression_codec, 7, length(compression_codec) - 7), compression_codec) AS codec_expression,
  '' AS ttl_expression
FROM system.columns WHERE database = {db:String} AND table = {table:String} ORDER BY position`

const showTablesSQL = `SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name`
const showDatabasesSQL = `SELECT name FROM system.databases ORDER BY name`

var showTablesRE = regexp.MustCompile("(?is)^\\s*SHOW\\s+TABLES\\s+FROM\\s+([A-Za-z_][A-Za-z0-9_]*|`[^`\\\\]+`)\\s*$")

// EncodeEscaped is DecodeString's inverse for the characters it must
// escape: how @clickhouse/client writes a string parameter.
func EncodeEscaped(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`, `'`, `\'`).Replace(s)
}

// Prepare turns one HyperDX statement into what the service runs.
// defaultFormat is the request's default_format setting, used when the
// statement has no FORMAT.
func (t Tables) Prepare(sql string, params map[string]string, defaultFormat string) (*Statement, error) {
	s, err := scan(sql)
	if err != nil {
		return nil, err
	}
	format := s.stripFormat()
	if format == "" {
		format = defaultFormat
	}
	if format == "" {
		format = "TabSeparated"
	}
	if !Formats[format] {
		return nil, refuse(400, 73, "UNKNOWN_FORMAT", "format", "format %s is not one the adapter renders (JSON, JSONEachRow, JSONCompact, JSONCompactEachRow[WithNames[AndTypes]])", format)
	}
	// SHOW TABLES FROM db: the parser does not take it
	if m := showTablesRE.FindStringSubmatch(s.text); m != nil {
		name := strings.Trim(m[1], "`")
		if p, ok := s.masks[name]; ok {
			if p.typ != "Identifier" {
				return nil, paramErr("SHOW TABLES FROM takes an Identifier")
			}
			raw, ok := params[p.name]
			if !ok {
				return nil, refuse(400, 456, "UNKNOWN_QUERY_PARAMETER", "missing_param", "substitution %s is not set", p.name)
			}
			if _, err := identValue(p, raw); err != nil {
				return nil, err
			}
			name = raw
		}
		return t.metadata("show_tables", showTablesSQL, map[string]string{"db": EncodeEscaped(name)}, format)
	}
	root, _, err := Bind(sql, params)
	if err != nil {
		return nil, err
	}
	switch x := root.(type) {
	case *chp.SelectQuery:
		st := &Statement{Kind: "select", SQL: chp.Format(x), Format: format, Metadata: t.onlySystem(x)}
		if !st.Metadata && !hasJoin(x) {
			st.Window = t.DeriveWindow(x)
		}
		return st, nil
	case *chp.DescribeStmt:
		if x.Target == nil || x.Target.Table == nil {
			return nil, syntaxErr("DESCRIBE without a table")
		}
		db := t.DefaultDatabase
		if x.Target.Database != nil {
			db = x.Target.Database.Name
		}
		return t.metadata("describe", DescribeSQL, map[string]string{"db": EncodeEscaped(db), "table": EncodeEscaped(x.Target.Table.Name)}, format)
	case *chp.ShowStmt:
		if strings.EqualFold(strings.TrimSpace(x.ShowType), "DATABASES") && x.LikePattern == nil && x.Limit == nil && x.OutFile == nil {
			return t.metadata("show_databases", showDatabasesSQL, nil, format)
		}
		return nil, notAllowed("not_select", "SHOW %s is not served", x.ShowType)
	case *chp.ExplainStmt:
		return nil, notAllowed("explain", "EXPLAIN is not served: its estimates are not scoped to the caller")
	}
	return nil, notAllowed("not_select", "only SELECT, DESCRIBE TABLE, SHOW DATABASES and SHOW TABLES are served, not %T", root)
}

func (t Tables) metadata(kind, tmpl string, params map[string]string, format string) (*Statement, error) {
	root, _, err := Bind(tmpl, params)
	if err != nil {
		return nil, err
	}
	return &Statement{Kind: kind, SQL: chp.Format(root), Format: format, Metadata: true}, nil
}

// onlySystem: every table read is in the system database or a CTE.
func (t Tables) onlySystem(root chp.Expr) bool {
	ctes := map[string]bool{}
	var tables []*chp.TableIdentifier
	walkAll(reflect.ValueOf(root), func(n any) {
		switch x := n.(type) {
		case *chp.CTEStmt:
			if id, ok := x.Expr.(*chp.Ident); ok {
				ctes[id.Name] = true
			}
		case *chp.TableIdentifier:
			tables = append(tables, x)
		case *chp.TableFunctionExpr:
			tables = append(tables, &chp.TableIdentifier{Table: &chp.Ident{Name: "?"}})
		}
	})
	for _, ti := range tables {
		if ti.Table == nil {
			return false
		}
		if ti.Database == nil {
			if ctes[ti.Table.Name] {
				continue
			}
			if t.DefaultDatabase != "system" {
				return false
			}
			continue
		}
		if ti.Database.Name != "system" {
			return false
		}
	}
	return true
}

func hasJoin(root chp.Expr) bool {
	found := false
	walkAll(reflect.ValueOf(root), func(n any) {
		if _, ok := n.(*chp.JoinExpr); ok {
			found = true
		}
	})
	return found
}
