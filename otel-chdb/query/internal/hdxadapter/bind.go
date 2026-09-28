// Package hdxadapter speaks enough of ClickHouse's HTTP interface for
// HyperDX's @clickhouse/client (node) and @clickhouse/client-web (browser),
// and sends every statement to the query service's POST /v1/query with the
// caller's own bearer token. It holds no ClickHouse credentials: the service
// is the only thing that reaches ClickHouse, so every statement is parsed,
// allow-listed, rebuilt, scoped to the caller and audited there (README.md).
//
// What the adapter does to a statement before the service sees it:
//
//   - binds HyperDX's query parameters ({HYPERDX_PARAM_n:Type}) as typed
//     literals in the parsed tree: a placeholder is masked by an identifier,
//     the statement is parsed, and each mask is replaced by a literal node
//     built from the decoded value (a String is a string literal, an Int64 is
//     toInt64(n), an Identifier is a quoted identifier). The value's text is
//     never pasted into the statement's text;
//   - takes the trailing FORMAT off (the client always appends one) and
//     renders the service's answer in that format;
//   - turns DESCRIBE and SHOW into SELECTs on the system tables the service
//     serves as metadata;
//   - derives the statement's time window from its own bounds when that is
//     provably a no-op on the rows (window.go), so the completeness label
//     can say "complete" (once complete_through, custody time, is past the
//     window's end + max_lateness: X-Otel-Settled-Through, event time, is
//     at or after it). A metadata statement's columns are the service's
//     allow-list: `SELECT *` on system.tables answers only those.
package hdxadapter

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Error is a refusal or failure, with the ClickHouse error code and name the
// client parses from the body (Code: N. DB::Exception: ... (NAME)).
type Error struct {
	HTTPStatus int
	Code       int
	Name       string
	Reason     string // stable token: metrics, logs, tests
	Message    string
}

func (e *Error) Error() string { return e.Reason + ": " + e.Message }

func refuse(status, code int, name, reason, format string, a ...any) *Error {
	return &Error{HTTPStatus: status, Code: code, Name: name, Reason: reason, Message: fmt.Sprintf(format, a...)}
}

func syntaxErr(format string, a ...any) *Error {
	return refuse(400, 62, "SYNTAX_ERROR", "parse_error", format, a...)
}

func paramErr(format string, a ...any) *Error {
	return refuse(400, 457, "BAD_QUERY_PARAMETER", "bad_param", format, a...)
}

func notAllowed(reason, format string, a ...any) *Error {
	return refuse(403, 497, "ACCESS_DENIED", reason, format, a...)
}

// maskPrefix names the identifiers that stand in for placeholders while the
// statement is parsed. A statement that already contains it is refused.
const maskPrefix = "hdxqp"

type placeholder struct {
	name, typ string
}

var placeholderRE = regexp.MustCompile(`^\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*:\s*([A-Za-z][A-Za-z0-9_(), ]*?)\s*\}`)

// word is an unquoted word in code (not in a string, a quoted identifier or
// a comment), with its byte span in the masked text.
type word struct {
	start, end int
	text       string
}

// scanned is a statement with its placeholders masked.
type scanned struct {
	text  string
	masks map[string]placeholder // mask identifier -> placeholder
	words []word                 // unquoted words in code, in order
	// lastCode is the end of the last non-space code byte (for the trailing
	// FORMAT).
	lastCode int
}

// scan masks every {name:Type} placeholder that is in code and records the
// unquoted words. Where ClickHouse's lexer and the parser's lexer are known
// to read a text differently it refuses, so that what is parsed here is what
// ClickHouse would have run: quoted identifiers with a backslash or a doubled
// quote, nested comments, '#' comments and $heredoc$ strings.
func scan(sql string) (*scanned, error) { return scanText(sql, true) }

func scanText(sql string, reserved bool) (*scanned, error) {
	if reserved && strings.Contains(strings.ToLower(sql), maskPrefix) {
		return nil, syntaxErr("the statement contains the adapter's reserved name %q", maskPrefix)
	}
	s := &scanned{masks: map[string]placeholder{}}
	var b strings.Builder
	b.Grow(len(sql) + 64)
	n := len(sql)
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == '\'':
			j := i + 1
			for ; j < n; j++ {
				if sql[j] == '\\' {
					j++
					continue
				}
				if sql[j] == '\'' {
					if j+1 < n && sql[j+1] == '\'' {
						j++
						continue
					}
					break
				}
			}
			if j >= n {
				return nil, syntaxErr("unterminated string literal")
			}
			b.WriteString(sql[i : j+1])
			s.lastCode = b.Len()
			i = j + 1
		case c == '`' || c == '"':
			j := i + 1
			for ; j < n && sql[j] != c; j++ {
				if sql[j] == '\\' {
					return nil, notAllowed("bad_identifier", "a quoted identifier with a backslash is not passed")
				}
			}
			if j >= n {
				return nil, syntaxErr("unterminated quoted identifier")
			}
			if j+1 < n && sql[j+1] == c {
				return nil, notAllowed("bad_identifier", "a quoted identifier with a doubled quote is not passed")
			}
			b.WriteString(sql[i : j+1])
			s.lastCode = b.Len()
			i = j + 1
		case c == '-' && i+1 < n && sql[i+1] == '-':
			j := strings.IndexByte(sql[i:], '\n')
			if j < 0 {
				j = n - i
			}
			b.WriteString(sql[i : i+j])
			i += j
		case c == '/' && i+1 < n && sql[i+1] == '*':
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				return nil, syntaxErr("unterminated comment")
			}
			body := sql[i+2 : i+2+j]
			if strings.Contains(body, "/*") {
				return nil, syntaxErr("nested comments are not supported")
			}
			b.WriteString(sql[i : i+2+j+2])
			i += 2 + j + 2
		case c == '#' || c == '$':
			return nil, syntaxErr("%q outside a string is not supported", string(c))
		case c == '{':
			m := placeholderRE.FindStringSubmatch(sql[i:])
			if m == nil {
				b.WriteByte(c)
				s.lastCode = b.Len()
				i++
				continue
			}
			mask := fmt.Sprintf("%s%d_", maskPrefix, len(s.masks))
			s.masks[mask] = placeholder{name: m[1], typ: strings.Join(strings.Fields(m[2]), "")}
			s.words = append(s.words, word{start: b.Len(), end: b.Len() + len(mask), text: mask})
			b.WriteString(mask)
			s.lastCode = b.Len()
			i += len(m[0])
		case isWordStart(c):
			j := i + 1
			for j < n && isWordByte(sql[j]) {
				j++
			}
			s.words = append(s.words, word{start: b.Len(), end: b.Len() + j - i, text: sql[i:j]})
			b.WriteString(sql[i:j])
			s.lastCode = b.Len()
			i = j
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			s.lastCode = b.Len()
			i++
		}
	}
	s.text = b.String()
	return s, nil
}

func isWordStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isWordByte(c byte) bool { return isWordStart(c) || (c >= '0' && c <= '9') }

// stripFormat removes a trailing `FORMAT <name>` (and a trailing ';') from
// the masked text and returns the format name ("" if there is none).
func (s *scanned) stripFormat() string {
	t := strings.TrimRight(s.text, " \t\n\r\f\v;")
	if len(s.words) < 2 {
		return ""
	}
	name, kw := s.words[len(s.words)-1], s.words[len(s.words)-2]
	if name.end != len(t) || !strings.EqualFold(kw.text, "FORMAT") || strings.HasPrefix(name.text, maskPrefix) {
		return ""
	}
	// nothing but space between FORMAT and the name
	if strings.TrimSpace(t[kw.end:name.start]) != "" {
		return ""
	}
	s.text = t[:kw.start]
	s.words = s.words[:len(s.words)-2]
	return name.text
}

// DecodeString decodes a parameter value the way ClickHouse 26.10 reads a
// String query parameter (the "escaped" text format, which is how
// @clickhouse/client sends strings; measured one escape at a time against
// the server, see bind_test.go): \b \f \n \r \t \v \a \e \0 are control
// characters, \xHH is a byte, \N is nothing, a backslash before " ' / = \
// ` or a control byte (0x00-0x1F) is dropped, and before anything else (DEL
// and bytes >= 0x80 included) it is kept. Where ClickHouse's
// answer is not a decoding (\x with a non-hex digit gives an arbitrary byte;
// a trailing backslash is an error) the value is refused. A raw tab or
// newline ends the value in that format, and ClickHouse refuses what follows
// (found by the differential test against the server): refused here too.
func DecodeString(v string) (string, error) {
	if !strings.ContainsAny(v, "\\\t\n") {
		return v, nil
	}
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\t' || c == '\n' {
			return "", paramErr("a string parameter has a raw tab or newline (the client sends them as \\t and \\n)")
		}
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if i+1 >= len(v) {
			return "", paramErr("a string parameter ends with a backslash")
		}
		i++
		e := v[i]
		switch e {
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'v':
			b.WriteByte('\v')
		case 'a':
			b.WriteByte(7)
		case 'e':
			b.WriteByte(0x1b)
		case '0':
			b.WriteByte(0)
		case 'N':
		case '"', '\'', '/', '=', '\\', '`':
			b.WriteByte(e)
		case 'x':
			if i+2 >= len(v) || !isHex(v[i+1]) || !isHex(v[i+2]) {
				return "", paramErr(`a string parameter has \x without two hex digits`)
			}
			x, _ := strconv.ParseUint(v[i+1:i+3], 16, 8)
			b.WriteByte(byte(x))
			i += 2
		default:
			if e < 0x20 { // a control byte: the backslash is dropped
				b.WriteByte(e)
				continue
			}
			b.WriteByte('\\')
			b.WriteByte(e)
		}
	}
	return b.String(), nil
}

func isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// literalText renders s as the inside of a ClickHouse string literal: a
// backslash and a quote escaped, control bytes, DEL and bytes that are not
// valid UTF-8 as \xHH, everything else as itself. ClickHouse and the
// parser read it back as exactly s.
func literalText(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\':
			b.WriteString(`\\`)
			i++
		case c == '\'':
			b.WriteString(`\'`)
			i++
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, `\x%02X`, c)
			i++
		case c < 0x80:
			b.WriteByte(c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size <= 1 {
				fmt.Fprintf(&b, `\x%02X`, c)
				i++
				continue
			}
			b.WriteString(s[i : i+size])
			i += size
		}
	}
	return b.String()
}

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// intRE: what ClickHouse accepts for an integer parameter (an optional sign
// and decimal digits; no spaces, hex, exponent or fraction).
var intRE = regexp.MustCompile(`^[+-]?[0-9]+$`)

// floatRE: decimal floats only; ClickHouse's inf/nan and hex floats are
// refused.
var floatRE = regexp.MustCompile(`^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$`)

var intBits = map[string]int{"Int8": 8, "Int16": 16, "Int32": 32, "Int64": 64, "UInt8": 8, "UInt16": 16, "UInt32": 32, "UInt64": 64}

// literal is a placeholder's value as a node of the tree: nil with a *Error
// for a value or type the adapter does not bind. Identifier is handled by
// the caller (it renames an identifier node).
func literal(p placeholder, raw string) (chp.Expr, error) {
	switch {
	case p.typ == "String":
		v, err := DecodeString(raw)
		if err != nil {
			return nil, err
		}
		return &chp.StringLiteral{Literal: literalText(v)}, nil
	case intBits[p.typ] > 0:
		bits := intBits[p.typ]
		if !intRE.MatchString(raw) {
			return nil, paramErr("value %q of %s is not an integer", raw, p.name)
		}
		var text string
		if strings.HasPrefix(p.typ, "U") {
			u, err := strconv.ParseUint(strings.TrimPrefix(raw, "+"), 10, bits)
			if err != nil {
				// ClickHouse wraps an out-of-range value; the adapter refuses it
				return nil, paramErr("value %q of %s is out of range for %s", raw, p.name, p.typ)
			}
			text = strconv.FormatUint(u, 10)
		} else {
			n, err := strconv.ParseInt(raw, 10, bits)
			if err != nil {
				return nil, paramErr("value %q of %s is out of range for %s", raw, p.name, p.typ)
			}
			text = strconv.FormatInt(n, 10)
		}
		return parseExpr("to" + p.typ + "(" + text + ")")
	case p.typ == "Float32" || p.typ == "Float64":
		bits := 64
		if p.typ == "Float32" {
			bits = 32
		}
		if !floatRE.MatchString(raw) {
			return nil, paramErr("value %q of %s is not a decimal number", raw, p.name)
		}
		f, err := strconv.ParseFloat(raw, bits)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return nil, paramErr("value %q of %s is out of range for %s", raw, p.name, p.typ)
		}
		text := strconv.FormatFloat(f, 'e', -1, bits)
		return parseExpr("to" + p.typ + "('" + text + "')")
	}
	return nil, paramErr("parameter %s has type %s, which the adapter does not bind (String, Identifier, [U]Int8-64, Float32/64)", p.name, p.typ)
}

// parseExpr parses an expression the adapter itself wrote from canonical
// text (a function of one number or one number-shaped string).
func parseExpr(s string) (chp.Expr, error) {
	st, err := chp.NewParser("SELECT " + s).ParseStmts()
	if err != nil || len(st) != 1 {
		return nil, fmt.Errorf("hdxadapter: %q does not parse: %v", s, err)
	}
	sq := st[0].(*chp.SelectQuery)
	return sq.SelectItems[0].Expr, nil
}

// identValue checks an Identifier parameter's value. ClickHouse takes it as
// one identifier, raw (a.b is one name with a dot, not a database and a
// table). Values the service does not pass in a quoted identifier are
// refused here.
func identValue(p placeholder, raw string) (*chp.Ident, error) {
	if raw == "" || strings.ContainsAny(raw, "`\"\\\x00\n\r") || !utf8.ValidString(raw) {
		return nil, paramErr("value %q of identifier %s is not one the adapter binds", raw, p.name)
	}
	if identRE.MatchString(raw) {
		return &chp.Ident{Name: raw, QuoteType: chp.BackTicks}, nil
	}
	return &chp.Ident{Name: raw, QuoteType: chp.BackTicks}, nil
}

var (
	exprType  = reflect.TypeOf((*chp.Expr)(nil)).Elem()
	identType = reflect.TypeOf(&chp.Ident{})
)

// binder substitutes the masks in a parsed statement.
type binder struct {
	masks  map[string]placeholder
	params map[string]string
	used   map[string]bool
	err    error
}

func (bd *binder) value(mask string) (placeholder, string, bool) {
	p := bd.masks[mask]
	raw, ok := bd.params[p.name]
	if !ok {
		bd.fail(refuse(400, 456, "UNKNOWN_QUERY_PARAMETER", "missing_param", "substitution %s is not set", p.name))
		return p, "", false
	}
	bd.used[mask] = true
	return p, raw, true
}

func (bd *binder) fail(err error) {
	if bd.err == nil {
		bd.err = err
	}
}

func isMask(id *chp.Ident) bool { return id != nil && strings.HasPrefix(id.Name, maskPrefix) }

// bind walks every field of the tree by reflection (so a node kind the
// parser's own visitor skips is still seen) and replaces each mask: in a
// field that holds an expression, a value parameter becomes a literal node
// and an Identifier parameter a quoted identifier; in a field that holds an
// identifier (a table, a database, an alias, a column), only an Identifier
// parameter is allowed. A mask as a function's name is refused.
func (bd *binder) bind(v reflect.Value) {
	if bd.err != nil || !v.IsValid() {
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return
		}
		if fe, ok := v.Interface().(*chp.FunctionExpr); ok && isMask(fe.Name) {
			bd.fail(notAllowed("param_function_name", "a parameter as a function name is not bound"))
			return
		}
		if tf, ok := v.Interface().(*chp.TableFunctionExpr); ok {
			if n, ok := tf.Name.(*chp.Ident); ok && isMask(n) {
				bd.fail(notAllowed("param_function_name", "a parameter as a table function name is not bound"))
				return
			}
		}
		bd.bind(v.Elem())
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		bd.bind(v.Elem())
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if !f.CanSet() {
				continue
			}
			bd.slot(f)
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			bd.slot(v.Index(i))
		}
	}
}

// slot handles one settable field or slice element.
func (bd *binder) slot(f reflect.Value) {
	switch {
	case f.Type() == identType:
		id, _ := f.Interface().(*chp.Ident)
		if !isMask(id) {
			return
		}
		p, raw, ok := bd.value(id.Name)
		if !ok {
			return
		}
		if p.typ != "Identifier" {
			bd.fail(paramErr("parameter %s of type %s stands where only an identifier can", p.name, p.typ))
			return
		}
		nid, err := identValue(p, raw)
		if err != nil {
			bd.fail(err)
			return
		}
		f.Set(reflect.ValueOf(nid))
	case f.Kind() == reflect.Interface && f.Type().Implements(exprType) || f.Type() == exprType:
		if f.IsNil() {
			return
		}
		id, ok := f.Interface().(*chp.Ident)
		if !ok || !isMask(id) {
			bd.bind(f)
			return
		}
		p, raw, ok := bd.value(id.Name)
		if !ok {
			return
		}
		var node chp.Expr
		var err error
		if p.typ == "Identifier" {
			node, err = identValue(p, raw)
		} else {
			node, err = literal(p, raw)
		}
		if err != nil {
			bd.fail(err)
			return
		}
		if !reflect.TypeOf(node).AssignableTo(f.Type()) {
			bd.fail(paramErr("parameter %s cannot stand here", p.name))
			return
		}
		f.Set(reflect.ValueOf(node))
	default:
		bd.bind(f)
	}
}

// Bind parses sql (HyperDX's statement, with placeholders), substitutes the
// parameters and returns the tree, the format the caller asked for and
// whether a FORMAT clause was present.
func Bind(sql string, params map[string]string) (chp.Expr, string, error) {
	s, err := scan(sql)
	if err != nil {
		return nil, "", err
	}
	format := s.stripFormat()
	stmts, err := chp.NewParser(s.text).ParseStmts()
	if err != nil {
		return nil, "", syntaxErr("%v", firstLine(err.Error()))
	}
	if len(stmts) != 1 {
		return nil, "", syntaxErr("exactly one statement, got %d", len(stmts))
	}
	bd := &binder{masks: s.masks, params: params, used: map[string]bool{}}
	root := stmts[0]
	holder := struct{ E chp.Expr }{root}
	hv := reflect.ValueOf(&holder).Elem()
	bd.slot(hv.Field(0))
	if bd.err != nil {
		return nil, "", bd.err
	}
	root = holder.E
	for m := range s.masks {
		if !bd.used[m] {
			// the parser kept the placeholder somewhere the walk cannot
			// replace it (inside a type, a keyword position, ...)
			return nil, "", paramErr("parameter %s stands where the adapter cannot bind it", s.masks[m].name)
		}
	}
	// what will be sent must not name a mask anywhere
	out, err := scanText(chp.Format(root), false)
	if err != nil {
		return nil, "", syntaxErr("the bound statement does not scan: %v", err)
	}
	for _, w := range out.words {
		if strings.HasPrefix(strings.ToLower(w.text), maskPrefix) {
			return nil, "", paramErr("a parameter was not bound")
		}
	}
	return root, format, nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

var errNotSelect = errors.New("not a select")
