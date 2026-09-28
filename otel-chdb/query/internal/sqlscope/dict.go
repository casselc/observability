package sqlscope

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// Dictionary is one ClickHouse dictionary a statement may read with
// dictGet(name, 'attribute', key) or dictHas(name, key): the entity
// catalog's normalized dictionaries (entities/sql/dictionaries.sql), which
// the entity rewrite proxy's value expressions read (entities/rwproxy).
//
// Every other dictionary, every other dict* / joinGet function, and any
// call whose name or attribute is not a plain string literal stays refused
// (denied_function): names are data, so they are allow-listed one by one.
//
// A dictionary is keyed by a hash (resource_id is the content hash of a
// resource's attributes, a pod key the hash of its uid), so a caller who
// knows or guesses another cluster's attributes can compute its key and
// probe for it: dictHas answers "exists", dictGet the entry. The rows a
// statement reads are scoped (additional_table_filters), but a lookup's key
// can be any expression, a literal included. So for a caller without every
// cluster and namespace, every lookup is guarded (Finish):
//
//	dictGet(d, 'a', k)  ->  if(<k in the caller's scope>, dictGet(d, 'a', k), CAST(<a's default>, '<a's type>'))
//	dictHas(d, k)       ->  and(<k in the caller's scope>, dictHas(d, k))
//
// An out-of-scope key reads exactly as an absent one: the attribute's
// declared default (what dictGet returns for a missing key), of the same
// type. "In scope" is decided from the dictionaries themselves:
//
//   - a root dictionary (d_res) has cluster_expr / namespace_expr, trusted
//     expressions over {key} naming the cluster and namespace of the entry
//     the key selects (d_res -> d_pod -> d_cluster's k8s.cluster.name);
//   - every other dictionary is derived: its key must be the value of an
//     attribute of an allow-listed dictionary whose ref names it
//     (dictGet('d_ns', 'attrs', dictGet('d_pod', 'ns_key', …))), and the
//     lookup is guarded with the guard of the lookup that produced its key.
//     A literal or computed key on a derived dictionary is refused
//     (dict_key), for every caller: nothing ties such a key to a cluster.
type Dictionary struct {
	// Name is database.dictionary, exactly as dictGet's first argument
	// names it.
	Name string `json:"name"`
	// Attributes are the attributes a statement may read, with the type
	// and default the dictionary declares for each.
	Attributes map[string]DictAttribute `json:"attributes"`
	// Cluster / Namespace (root dictionaries only): expressions over {key}.
	Cluster   string `json:"cluster_expr"`
	Namespace string `json:"namespace_expr"`
}

// DictAttribute is one attribute of a Dictionary.
type DictAttribute struct {
	// Type is the attribute's ClickHouse type (UInt64, String, UUID, …).
	Type string `json:"type"`
	// Default is a SQL literal: what dictGet returns for a key the
	// dictionary does not hold (its DEFAULT, else the type's default).
	Default string `json:"default"`
	// Ref names the dictionary this attribute is a key of (d_res.pod_key ->
	// d_pod), so that dictionary may be read with it.
	Ref string `json:"ref"`
}

const keyPlaceholder = "__qs_dict_key__"

var (
	dictNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)
	dictTypeRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*(\([A-Za-z0-9_, ']*\))?$`)
)

// SetDictionaries allow-lists ds (trusted configuration), replacing any
// earlier list.
func (p *Policy) SetDictionaries(ds []*Dictionary) error {
	m := map[string]*Dictionary{}
	for _, d := range ds {
		if !dictNameRE.MatchString(d.Name) {
			return fmt.Errorf("dictionary %q: the name must be database.dictionary", d.Name)
		}
		if _, dup := m[d.Name]; dup {
			return fmt.Errorf("dictionary %s is listed twice", d.Name)
		}
		if len(d.Attributes) == 0 {
			return fmt.Errorf("dictionary %s: no attributes", d.Name)
		}
		for a, at := range d.Attributes {
			if !nameRE.MatchString(a) {
				return fmt.Errorf("dictionary %s: attribute %q is not a plain name", d.Name, a)
			}
			if !dictTypeRE.MatchString(at.Type) {
				return fmt.Errorf("dictionary %s.%s: type %q", d.Name, a, at.Type)
			}
			e, err := parseExpr(at.Default)
			if err != nil {
				return fmt.Errorf("dictionary %s.%s: default: %w", d.Name, a, err)
			}
			if err := fixedPoint("SELECT " + chp.Format(castTo(e, at.Type))); err != nil {
				return fmt.Errorf("dictionary %s.%s: default %q as %s: %v", d.Name, a, at.Default, at.Type, err)
			}
		}
		if (d.Cluster == "") != (d.Namespace == "") {
			return fmt.Errorf("dictionary %s: a root dictionary needs both cluster_expr and namespace_expr", d.Name)
		}
		for _, x := range []string{d.Cluster, d.Namespace} {
			if x != "" && !strings.Contains(x, "{key}") {
				return fmt.Errorf("dictionary %s: %q does not use {key}", d.Name, x)
			}
		}
		m[d.Name] = d
	}
	referenced := map[string]bool{}
	for _, d := range m {
		for a, at := range d.Attributes {
			if at.Ref == "" {
				continue
			}
			if _, ok := m[at.Ref]; !ok {
				return fmt.Errorf("dictionary %s.%s: ref %s is not an allow-listed dictionary", d.Name, a, at.Ref)
			}
			referenced[at.Ref] = true
		}
	}
	for _, d := range m {
		if !d.root() && !referenced[d.Name] {
			return fmt.Errorf("dictionary %s: neither a root (cluster_expr) nor referenced by another dictionary's attribute: no key could reach it", d.Name)
		}
	}
	old := p.Dictionaries
	p.Dictionaries = m
	// the guard expressions must pass the same checks a statement does:
	// they end up in one
	for _, d := range m {
		for _, x := range []string{d.Cluster, d.Namespace} {
			if x == "" {
				continue
			}
			e, err := d.template(x, &chp.Ident{Name: "k", QuoteType: chp.Unquoted})
			if err == nil {
				_, err = p.Prepare("SELECT " + chp.Format(e))
			}
			if err != nil {
				p.Dictionaries = old
				return fmt.Errorf("dictionary %s: scope expression %q: %v", d.Name, x, err)
			}
		}
	}
	return nil
}

func (d *Dictionary) root() bool { return d.Cluster != "" }

// template parses x with {key} standing for key (a node shared, not
// copied: the formatter writes it where it stands).
func (d *Dictionary) template(x string, key chp.Expr) (chp.Expr, error) {
	e, err := parseExpr(strings.ReplaceAll(x, "{key}", keyPlaceholder))
	if err != nil {
		return nil, err
	}
	if id, ok := e.(*chp.Ident); ok && id.Name == keyPlaceholder {
		return key, nil
	}
	n := replaceIdent(reflect.ValueOf(e), keyPlaceholder, key)
	if n == 0 {
		return nil, fmt.Errorf("{key} is not an expression of its own in %q", x)
	}
	return e, nil
}

// replaceIdent sets every settable Expr field (or slice element) that holds
// the identifier name to with; it returns how many it replaced.
func replaceIdent(v reflect.Value, name string, with chp.Expr) int {
	n := 0
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return 0
		}
		if id, ok := v.Interface().(*chp.Ident); ok && id.Name == name && v.CanSet() && v.Type() == exprType {
			v.Set(reflect.ValueOf(with))
			return 1
		}
		n += replaceIdent(v.Elem(), name, with)
	case reflect.Pointer:
		if !v.IsNil() {
			n += replaceIdent(v.Elem(), name, with)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				n += replaceIdent(v.Field(i), name, with)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			n += replaceIdent(v.Index(i), name, with)
		}
	}
	return n
}

func castTo(e chp.Expr, typ string) chp.Expr {
	return fn("CAST", e, &chp.StringLiteral{Literal: typ})
}

// dictCall is a dictGet / dictHas call that passed the checks.
type dictCall struct {
	f    *chp.FunctionExpr
	d    *Dictionary
	attr string // "" for dictHas
	key  chp.Expr
}

// isDictFunc: the two functions that may read an allow-listed dictionary.
func isDictFunc(name string) (get, ok bool) {
	switch {
	case strings.EqualFold(name, "dictGet"):
		return true, true
	case strings.EqualFold(name, "dictHas"):
		return false, true
	}
	return false, false
}

// funcArgs are f's arguments, unwrapped; nil and false for a parametric
// call or an aliased argument.
func funcArgs(f *chp.FunctionExpr) ([]chp.Expr, bool) {
	if f.Params == nil {
		return nil, true
	}
	if f.Params.ColumnArgList != nil {
		return nil, false
	}
	if f.Params.Items == nil {
		return nil, true
	}
	out := make([]chp.Expr, 0, len(f.Params.Items.Items))
	for _, it := range f.Params.Items.Items {
		if c, ok := it.(*chp.ColumnExpr); ok {
			if c.Alias != nil {
				return nil, false
			}
			it = c.Expr
		}
		out = append(out, it)
	}
	return out, true
}

// plainString is a string literal's value when it has no escape at all
// (so the parser's text and ClickHouse's value are the same bytes).
func plainString(e chp.Expr) (string, bool) {
	s, ok := e.(*chp.StringLiteral)
	if !ok || strings.ContainsAny(s.Literal, `\'`) {
		return "", false
	}
	return s.Literal, true
}

// asDictCall checks one dictGet / dictHas call: an allow-listed dictionary
// and attribute named by plain literals, the right arity, and for a
// derived dictionary a key that is an allow-listed reference to it.
func (p *Policy) asDictCall(f *chp.FunctionExpr) (*dictCall, error) {
	get, ok := isDictFunc(f.Name.Name)
	if !ok {
		return nil, nil
	}
	args, ok := funcArgs(f)
	want := 2
	if get {
		want = 3
	}
	if !ok || len(args) != want {
		return nil, reject("denied_function", "%s takes %d plain arguments here", f.Name.Name, want)
	}
	name, ok := plainString(args[0])
	if !ok {
		return nil, reject("denied_function", "%s: the dictionary must be named by a plain string literal", f.Name.Name)
	}
	d := p.Dictionaries[name]
	if d == nil {
		return nil, reject("denied_function", "dictionary %q is not one the service serves", name)
	}
	c := &dictCall{f: f, d: d, key: args[len(args)-1]}
	if get {
		a, ok := plainString(args[1])
		if !ok {
			return nil, reject("denied_function", "dictGet: the attribute must be named by a plain string literal")
		}
		if _, ok := d.Attributes[a]; !ok {
			return nil, reject("denied_function", "attribute %q of dictionary %s is not served", a, name)
		}
		c.attr = a
	}
	if !d.root() {
		if _, err := p.parentOf(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// parentOf is the lookup that produced a derived call's key.
func (p *Policy) parentOf(c *dictCall) (*dictCall, error) {
	pf, ok := c.key.(*chp.FunctionExpr)
	if ok && pf.Name != nil {
		if get, isDict := isDictFunc(pf.Name.Name); isDict && get {
			parent, err := p.asDictCall(pf)
			if err != nil {
				return nil, err
			}
			if parent.d.Attributes[parent.attr].Ref == c.d.Name {
				return parent, nil
			}
		}
	}
	return nil, reject("dict_key", "dictionary %s is read only with a key another served dictionary gives for it (e.g. dictGet('…', '<key attribute>', …))", c.d.Name)
}

// guardDictionaries rewrites every dictionary lookup that decides a value
// (every one whose result is not only another lookup's key) so that a key
// outside s reads as an absent key. A no-op for a caller with every
// cluster and namespace.
func (pr *Prepared) guardDictionaries(s Scope) error {
	if s.AllClusters && s.AllNamespaces || len(pr.policy.Dictionaries) == 0 {
		return nil
	}
	var calls []*dictCall
	var err error
	visit(reflect.ValueOf(pr.root), func(n chp.Expr) bool {
		if err != nil {
			return false
		}
		if f, ok := n.(*chp.FunctionExpr); ok && f.Name != nil {
			var c *dictCall
			if c, err = pr.policy.asDictCall(f); c != nil {
				calls = append(calls, c)
			}
		}
		return err == nil
	})
	if err != nil {
		return err
	}
	// a derived call's key lookup only feeds that call, which is guarded
	keyOnly := map[*chp.FunctionExpr]bool{}
	for _, c := range calls {
		if !c.d.root() {
			if kf, ok := c.key.(*chp.FunctionExpr); ok {
				keyOnly[kf] = true
			}
		}
	}
	type change struct {
		c     *dictCall
		guard chp.Expr
	}
	var changes []change
	for _, c := range calls {
		if keyOnly[c.f] {
			continue
		}
		g, err := pr.guard(c, s)
		if err != nil {
			return err
		}
		changes = append(changes, change{c, g})
	}
	// guards first, then the rewrite: a guard is built from the tree as
	// the caller wrote it
	for _, ch := range changes {
		f := ch.c.f
		orig := &chp.FunctionExpr{Name: f.Name, Params: f.Params}
		if ch.c.attr == "" {
			f.Name = &chp.Ident{Name: "and", QuoteType: chp.Unquoted}
			f.Params = list([]chp.Expr{ch.guard, orig}).(*chp.ParamExprList)
			continue
		}
		at := ch.c.d.Attributes[ch.c.attr]
		def, _ := parseExpr(at.Default) // checked in SetDictionaries
		f.Name = &chp.Ident{Name: "if", QuoteType: chp.Unquoted}
		f.Params = list([]chp.Expr{ch.guard, orig, castTo(def, at.Type)}).(*chp.ParamExprList)
	}
	return nil
}

// guard is "c's key is in s": the root lookup's cluster and namespace
// expressions over its key, IN the caller's values.
func (pr *Prepared) guard(c *dictCall, s Scope) (chp.Expr, error) {
	for !c.d.root() {
		parent, err := pr.policy.parentOf(c)
		if err != nil {
			return nil, err
		}
		c = parent
	}
	var parts []chp.Expr
	if !s.AllClusters {
		vals, err := strs(s.Clusters, ClusterRE, "cluster")
		if err != nil {
			return nil, err
		}
		e, err := c.d.template(c.d.Cluster, c.key)
		if err != nil {
			return nil, reject("roundtrip", "dictionary %s: %v", c.d.Name, err)
		}
		parts = append(parts, in(e, vals))
	}
	if !s.AllNamespaces {
		vals, err := strs(s.Namespaces, NamespaceRE, "namespace")
		if err != nil {
			return nil, err
		}
		e, err := c.d.template(c.d.Namespace, c.key)
		if err != nil {
			return nil, reject("roundtrip", "dictionary %s: %v", c.d.Name, err)
		}
		parts = append(parts, in(e, vals))
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out = and(out, p)
	}
	return paren(out), nil
}

// DictionaryNames lists the allow-listed dictionaries, sorted.
func (p *Policy) DictionaryNames() []string {
	out := make([]string, 0, len(p.Dictionaries))
	for n := range p.Dictionaries {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
